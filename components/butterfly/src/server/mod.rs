// Copyright (c) 2016 Chef Software Inc. and/or applicable contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The SWIM server.
//!
//! Creates `Server` structs, that hold everything we need to run the SWIM protocol. Winds up with
//! 3 separate threads - inbound (incoming connections), outbound (the Probe protocl), and expire
//! (turning Suspect members into Confirmed members).

pub mod expire;
pub mod inbound;
pub mod outbound;
pub mod pull;
pub mod push;
pub mod timing;

use std::collections::HashSet;
use std::fmt;
use std::net::{ToSocketAddrs, UdpSocket, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::mpsc::channel;
use std::time::Duration;
use std::thread;

use habitat_core::service::ServiceGroup;

use error::{Result, Error};
use member::{Member, Health, MemberList};
use trace::{Trace, TraceKind};
use rumor::{Rumor, RumorStore, RumorList, RumorKey};
use service::Service;
use election::Election;
use message::swim::Election_Status;

/// The server struct. Is thread-safe.
#[derive(Debug, Clone)]
pub struct Server {
    pub name: Arc<String>,
    pub member_id: Arc<String>,
    pub member: Arc<RwLock<Member>>,
    pub member_list: MemberList,
    pub rumor_list: RumorList,
    pub service_store: RumorStore<Service>,
    pub election_store: RumorStore<Election>,
    pub swim_addr: Arc<RwLock<SocketAddr>>,
    pub gossip_addr: Arc<RwLock<SocketAddr>>,
    // These are all here for testing support
    pub pause: Arc<AtomicBool>,
    pub trace: Arc<RwLock<Trace>>,
    pub swim_rounds: Arc<AtomicIsize>,
    pub gossip_rounds: Arc<AtomicIsize>,
    pub blacklist: Arc<RwLock<HashSet<String>>>,
}

impl Server {
    /// Create a new server, bound to the `addr`, hosting a particular `member`, and with a
    /// `Trace` struct.
    pub fn new<A: ToSocketAddrs>(swim_addr: A,
                                 gossip_addr: A,
                                 member: Member,
                                 trace: Trace,
                                 name: Option<String>)
                                 -> Result<Server> {
        let swim_socket_addr = match swim_addr.to_socket_addrs() {
            Ok(mut addrs) => addrs.nth(0).unwrap(),
            Err(e) => return Err(Error::CannotBind(e)),
        };
        let gossip_socket_addr = match gossip_addr.to_socket_addrs() {
            Ok(mut addrs) => addrs.nth(0).unwrap(),
            Err(e) => return Err(Error::CannotBind(e)),
        };
        Ok(Server {
            name: Arc::new(name.unwrap_or(String::from(member.get_id()))),
            member_id: Arc::new(String::from(member.get_id())),
            member: Arc::new(RwLock::new(member)),
            member_list: MemberList::new(),
            rumor_list: RumorList::default(),
            service_store: RumorStore::default(),
            election_store: RumorStore::default(),
            swim_addr: Arc::new(RwLock::new(swim_socket_addr)),
            gossip_addr: Arc::new(RwLock::new(gossip_socket_addr)),
            pause: Arc::new(AtomicBool::new(false)),
            trace: Arc::new(RwLock::new(trace)),
            swim_rounds: Arc::new(AtomicIsize::new(0)),
            gossip_rounds: Arc::new(AtomicIsize::new(0)),
            blacklist: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    /// Every iteration of the outbound protocol (which means every member has been pinged if they
    /// are available) increments the round. If we exceed an isize in rounds, we reset to 0.
    ///
    /// This is useful in integration testing, to allow tests to time out after a worst-case
    /// boundary in rounds.
    pub fn swim_rounds(&self) -> isize {
        self.swim_rounds.load(Ordering::SeqCst)
    }

    /// Adds 1 to the current round, atomically.
    pub fn update_swim_round(&self) {
        let current_round = self.swim_rounds.load(Ordering::SeqCst);
        match current_round.checked_add(1) {
            Some(_number) => {
                self.swim_rounds.fetch_add(1, Ordering::SeqCst);
            }
            None => {
                debug!("Exceeded an isize integer in swim-rounds. Congratulations, this is a \
                        very long running supervisor!");
                self.swim_rounds.store(0, Ordering::SeqCst);
            }
        }
    }

    /// Every iteration of the gossip protocol (which means every member has been sent if they
    /// are available) increments the round. If we exceed an isize in rounds, we reset to 0.
    ///
    /// This is useful in integration testing, to allow tests to time out after a worst-case
    /// boundary in rounds.
    pub fn gossip_rounds(&self) -> isize {
        self.gossip_rounds.load(Ordering::SeqCst)
    }

    /// Adds 1 to the current round, atomically.
    pub fn update_gossip_round(&self) {
        let current_round = self.gossip_rounds.load(Ordering::SeqCst);
        match current_round.checked_add(1) {
            Some(_number) => {
                self.gossip_rounds.fetch_add(1, Ordering::SeqCst);
            }
            None => {
                debug!("Exceeded an isize integer in gossip-rounds. Congratulations, this is a \
                        very long running supervisor!");
                self.gossip_rounds.store(0, Ordering::SeqCst);
            }
        }
    }

    /// Start the server, aloung with a `Timing` for outbound connections. Spawns the `inbound`,
    /// `outbound`, and `expire` threads.
    ///
    /// # Errors
    ///
    /// * Returns `Error::CannotBind` if the socket cannot be bound
    /// * Returns `Error::SocketSetReadTimeout` if the socket read timeout cannot be set
    /// * Returns `Error::SocketSetWriteTimeout` if the socket write timeout cannot be set
    pub fn start(&self, timing: timing::Timing) -> Result<()> {
        let (tx_outbound, rx_inbound) = channel();

        let socket =
            match UdpSocket::bind(*self.swim_addr.read().expect("Swim address lock is poisoned")) {
                Ok(socket) => socket,
                Err(e) => return Err(Error::CannotBind(e)),
            };
        try!(socket.set_read_timeout(Some(Duration::from_millis(1000)))
            .map_err(|e| Error::SocketSetReadTimeout(e)));
        try!(socket.set_write_timeout(Some(Duration::from_millis(1000)))
            .map_err(|e| Error::SocketSetReadTimeout(e)));


        let server_a = self.clone();
        let socket_a = match socket.try_clone() {
            Ok(socket_a) => socket_a,
            Err(_) => return Err(Error::SocketCloneError),
        };
        let _ = thread::Builder::new().name(format!("inbound-{}", self.name())).spawn(move || {
            inbound::Inbound::new(&server_a, socket_a, tx_outbound).run();
            panic!("You should never, ever get here, judy");
        });

        let server_b = self.clone();
        let socket_b = match socket.try_clone() {
            Ok(socket_b) => socket_b,
            Err(_) => return Err(Error::SocketCloneError),
        };
        let timing_b = timing.clone();
        let _ = thread::Builder::new().name(format!("outbound-{}", self.name())).spawn(move || {
            outbound::Outbound::new(&server_b, socket_b, rx_inbound, timing_b).run();
            panic!("You should never, ever get here, bob");
        });

        let server_c = self.clone();
        let timing_c = timing.clone();
        let _ = thread::Builder::new().name(format!("expire-{}", self.name())).spawn(move || {
            expire::Expire::new(&server_c, timing_c).run();
            panic!("You should never, ever get here, frank");
        });

        let server_d = self.clone();
        let _ = thread::Builder::new().name(format!("pull-{}", self.name())).spawn(move || {
            pull::Pull::new(&server_d).run();
            panic!("You should never, ever get here, davey");
        });

        let server_e = self.clone();
        let _ = thread::Builder::new().name(format!("push-{}", self.name())).spawn(move || {
            push::Push::new(&server_e, timing).run();
            panic!("You should never, ever get here, liu");
        });

        Ok(())
    }

    /// Blacklist a given address, causing no traffic to be seen.
    pub fn add_to_blacklist(&self, member_id: String) {
        let mut blacklist = self.blacklist.write().expect("Write lock for blacklist is poisoned");
        blacklist.insert(member_id);
    }

    /// Remove a given address from the blacklist.
    pub fn remove_from_blacklist(&self, member_id: &str) {
        let mut blacklist = self.blacklist.write().expect("Write lock for blacklist is poisoned");
        blacklist.remove(member_id);
    }

    /// Check that a given address is on the blacklist.
    pub fn check_blacklist(&self, member_id: &str) -> bool {
        let blacklist = self.blacklist.write().expect("Write lock for blacklist is poisoned");
        blacklist.contains(member_id)
    }

    /// Stop the outbound and inbound threads from processing work.
    pub fn pause(&mut self) {
        self.pause.compare_and_swap(false, true, Ordering::Relaxed);
    }

    /// Allow the outbound and inbound threads to process work.
    pub fn unpause(&mut self) {
        self.pause.compare_and_swap(true, false, Ordering::Relaxed);
    }

    /// Whether this server is currently paused.
    pub fn paused(&self) -> bool {
        self.pause.load(Ordering::Relaxed)
    }

    /// Return the swim address we are bound to
    pub fn swim_addr(&self) -> SocketAddr {
        let sa = self.swim_addr.read().expect("Swim Address lock poisoned");
        sa.clone()
    }


    /// Return the port number of the swim socket we are bound to.
    pub fn swim_port(&self) -> u16 {
        self.swim_addr.read().expect("Swim Address lock poisoned").port()
    }

    /// Return the gossip address we are bound to
    pub fn gossip_addr(&self) -> SocketAddr {
        let ga = self.gossip_addr.read().expect("Gossip Address lock poisoned");
        ga.clone()
    }

    /// Return the port number of the gossip socket we are bound to.
    pub fn gossip_port(&self) -> u16 {
        self.gossip_addr.read().expect("Gossip Address lock poisoned").port()
    }

    /// Return the member ID of this server.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Return the name of this server.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Insert a member to the `MemberList`, and update its `RumorKey` appropriately.
    pub fn insert_member(&self, member: Member, health: Health) {
        let rk: RumorKey = RumorKey::from(&member);
        // NOTE: This sucks so much right here. Check out how we allocate no matter what, because
        // of just how the logic goes. The value of the trace is really high, though, so we suck it
        // for now.
        let trace_member_id = String::from(member.get_id());
        let trace_incarnation = member.get_incarnation();
        let trace_health = health.clone();
        if self.member_list.insert(member, health) {
            trace_it!(MEMBERSHIP: self,
                      TraceKind::MemberUpdate,
                      trace_member_id,
                      trace_incarnation,
                      trace_health);
            self.rumor_list.insert(rk);
        }
    }

    /// Change the helth of a `Member`, and update its `RumorKey`.
    pub fn insert_health(&self, member: &Member, health: Health) {
        let rk: RumorKey = RumorKey::from(&member);
        // NOTE: This sucks so much right here. Check out how we allocate no matter what, because
        // of just how the logic goes. The value of the trace is really high, though, so we suck it
        // for now.
        let trace_member_id = String::from(member.get_id());
        let trace_incarnation = member.get_incarnation();
        let trace_health = health.clone();
        if self.member_list.insert_health(member, health) {
            trace_it!(MEMBERSHIP: self,
                      TraceKind::MemberUpdate,
                      trace_member_id,
                      trace_incarnation,
                      trace_health);
            self.rumor_list.insert(rk);
        }
    }

    /// Given a membership record and some health, insert it into the Member List.
    pub fn insert_member_from_rumor(&self, member: Member, mut health: Health) {
        let mut incremented_incarnation = false;
        let rk: RumorKey = RumorKey::from(&member);
        if member.get_id() == self.member_id() {
            if health != Health::Alive {
                let mut me = self.member.write().expect("Member lock is poisoned");
                let mut incarnation = me.get_incarnation();
                incarnation += 1;
                me.set_incarnation(incarnation);
                health = Health::Alive;
                incremented_incarnation = true;
            }
        }
        // NOTE: This sucks so much right here. Check out how we allocate no matter what, because
        // of just how the logic goes. The value of the trace is really high, though, so we suck it
        // for now.
        let trace_member_id = String::from(member.get_id());
        let trace_incarnation = member.get_incarnation();
        let trace_health = health.clone();

        if self.member_list.insert(member, health) || incremented_incarnation {
            trace_it!(MEMBERSHIP: self,
                      TraceKind::MemberUpdate,
                      trace_member_id,
                      trace_incarnation,
                      trace_health);
            self.rumor_list.insert(rk);
        }
    }

    /// Insert members from a list of received rumors.
    pub fn insert_member_from_rumors(&self, members: Vec<(Member, Health)>) {
        for (member, health) in members.into_iter() {
            self.insert_member_from_rumor(member, health);
        }
    }

    /// Insert a service rumor into the service store.
    pub fn insert_service(&self, service: Service) {
        let rk = RumorKey::from(&service);
        if self.service_store.insert(service) {
            self.rumor_list.insert(rk);
        }
    }

    /// Get all the Member ID's who are present in a given service group.
    pub fn get_electorate(&self, key: &str) -> Vec<String> {
        let mut electorate = vec![];
        self.service_store.with_rumors(key, |s| {
            if self.member_list
                .check_health_of_by_id(s.get_member_id(), Health::Alive) {
                electorate.push(String::from(s.get_member_id()));
            }
        });
        electorate
    }

    /// Check if a given service group has quorum to run an election.
    ///
    /// A given group has quorum if, from this servers perspective, it has an alive population that
    /// is over 50%, and at least 3 members.
    pub fn check_quorum(&self, key: &str) -> bool {
        let electorate = self.get_electorate(key);

        let total_population = self.service_store.len_for_key(key);
        let alive_population = electorate.len();

        if total_population < 3 {
            info!("Quorum size: {}/3 - election cannot complete",
                  total_population);
            return false;
        }
        if total_population % 2 == 0 {
            warn!("This census has an even population. If half the membership fails, quorum will \
                   never be met, and no leader will be elected. Add another instance to the \
                   service group!");
        }

        let alive_pop = alive_population as f32;
        let total_pop = total_population as f32;
        let percent_alive: usize =
            ((alive_pop.round() / total_pop.round()) * 100.0).round() as usize;
        if percent_alive > 50 { true } else { false }
    }

    /// Start an election for the given service group, declaring this members suitability and the
    /// term for the election.
    pub fn start_election(&self, sg: ServiceGroup, suitability: u64, term: u64) {
        let mut e = Election::new(self.member_id(), sg, suitability);
        e.set_term(term);
        let ek = RumorKey::from(&e);
        if !self.check_quorum(e.key()) {
            e.no_quorum();
        }
        self.election_store
            .insert(e);
        self.rumor_list.insert(ek);
    }

    /// Check to see if this server needs to restart a given election. This happens when:
    ///
    /// a) We are the leader, and we have lost quorum with the rest of the group.
    /// b) We are not the leader, and we have detected that the leader is confirmed dead.
    pub fn restart_elections(&self) {
        let mut elections_to_restart = vec![];
        self.election_store.with_keys(|(service_group, rumors)| {
            if self.service_store.contains_rumor(&service_group, self.member_id()) {
                // This is safe; there is only one id for an election, and it is "election"
                let election = rumors.get("election")
                    .expect("Lost an election struct between looking it up and reading it.");
                // If we are finished, and the leader is dead, we should restart the election
                if election.get_member_id() == self.member_id() {
                    // If we are the leader, and we have lost quorum, we should restart the election
                    if self.check_quorum(election.key()) == false {
                        warn!("Restarting election with a new term as the leader has lost quorum: {:?}", election);
                        elections_to_restart.push((String::from(&service_group[..]), election.get_term()));

                    }
                } else if election.get_status() == Election_Status::Finished {
                    if self.member_list
                        .check_health_of_by_id(election.get_member_id(), Health::Confirmed) {
                            warn!("Restarting election with a new term as the leader is dead {}: {:?}", self.member_id(), election);
                            elections_to_restart.push((String::from(&service_group[..]), election.get_term()));
                    }
                }
            }
        });
        for (service_group, old_term) in elections_to_restart {
            let sg = match ServiceGroup::from_str(&service_group) {
                Ok(sg) => sg,
                Err(e) => {
                    error!("Failed to process service group from string '{}': {}",
                           service_group,
                           e);
                    return;
                }
            };
            let term = old_term + 1;
            warn!("Starting a new election for {} {}", sg, term);
            self.election_store.remove(&service_group, "election");
            self.start_election(sg, 0, term);
        }
    }

    /// Insert an election into the election store. Handles creating a new election rumor for this
    /// member on receipt of an election rumor for a service this server cares about. Also handles
    /// stopping the election if we are the winner and we have enough votes.
    pub fn insert_election(&self, mut election: Election) {
        let rk = RumorKey::from(&election);

        // If this is an election for a service group we care about
        if self.service_store.contains_rumor(election.get_service_group(), self.member_id()) {
            // And the election store already has an election rumor for this election
            if self.election_store.contains_rumor(election.key(), election.id()) {
                let mut new_term = false;
                self.election_store.with_rumor(election.key(), election.id(), |ce| {
                    new_term = election.get_term() > ce.unwrap().get_term()
                });
                if new_term {
                    self.election_store.remove(election.key(), election.id());
                    let sg = match ServiceGroup::from_str(election.get_service_group()) {
                        Ok(sg) => sg,
                        Err(e) => {
                            error!("Election malformed; cannot parse service group: {}", e);
                            return;
                        }
                    };
                    self.start_election(sg, 0, election.get_term());
                }
                // If we are the member that this election is voting for, then check to see if the election
                // is over! If it is, mark this election as final before you process it.
                if self.member_id() == election.get_member_id() {
                    if self.check_quorum(election.key()) {
                        let electorate = self.get_electorate(election.key());
                        let mut num_votes = 0;
                        for vote in election.get_votes().iter() {
                            if electorate.contains(vote) {
                                num_votes += 1;
                            }
                        }
                        if num_votes == electorate.len() {
                            debug!("Election is finished: {:#?}", election);
                            election.finish();
                        } else {
                            debug!("I have quorum, but election is not finished {}/{}",
                                   num_votes,
                                   electorate.len());
                        }
                    } else {
                        election.no_quorum();
                        warn!("Election lacks quorum: {:#?}", election);
                    }
                }
            } else {
                // Otherwise, we need to create a new election object for ourselves prior to
                // merging.
                let sg = match ServiceGroup::from_str(election.get_service_group()) {
                    Ok(sg) => sg,
                    Err(e) => {
                        error!("Election malformed; cannot parse service group: {}", e);
                        return;
                    }
                };
                self.start_election(sg, 0, election.get_term());
            }
            if !election.is_finished() {
                let has_quorum = self.check_quorum(election.key());
                if has_quorum {
                    election.running();
                } else {
                    election.no_quorum();
                }
            }
        }
        if self.election_store.insert(election) {
            self.rumor_list.insert(rk);
        }
    }
}

impl fmt::Display for Server {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "{}@{}/{}",
               self.name(),
               self.swim_port(),
               self.gossip_port())
    }
}

#[cfg(test)]
mod tests {
    mod server {
        use server::Server;
        use server::timing::Timing;
        use member::Member;
        use trace::Trace;
        use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};

        static SWIM_PORT: AtomicUsize = ATOMIC_USIZE_INIT;
        static GOSSIP_PORT: AtomicUsize = ATOMIC_USIZE_INIT;

        fn start_server() -> Server {
            SWIM_PORT.compare_and_swap(0, 6666, Ordering::Relaxed);
            GOSSIP_PORT.compare_and_swap(0, 7777, Ordering::Relaxed);
            let swim_port = SWIM_PORT.fetch_add(1, Ordering::Relaxed);
            let swim_listen = format!("127.0.0.1:{}", swim_port);
            let gossip_port = GOSSIP_PORT.fetch_add(1, Ordering::Relaxed);
            let gossip_listen = format!("127.0.0.1:{}", gossip_port);
            let mut member = Member::new();
            member.set_swim_port(swim_port as i32);
            member.set_gossip_port(gossip_port as i32);
            Server::new(&swim_listen[..],
                        &gossip_listen[..],
                        member,
                        Trace::default(),
                        None)
                .unwrap()
        }

        #[test]
        fn new() {
            start_server();
        }

        #[test]
        fn start_listener() {
            let server = start_server();
            server.start(Timing::default()).expect("Server failed to start");
        }
    }
}
