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

//! The push thread.
//!
//! This is the thread for distributing rumors to members. It distributes to `FANOUT` members, no
//! more often than `Timing::GOSSIP_PERIOD_DEFAULT_MS`.

use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use habitat_net::server::ZMQ_CONTEXT;
use protobuf::Message;
use time::SteadyTime;
use zmq;

use message::swim::{Rumor as ProtoRumor, Rumor_Type as ProtoRumor_Type, Member as ProtoMember,
                    Membership as ProtoMembership};
use rumor::{RumorKey, RumorVec};
use member::Member;
use server::Server;
use server::timing::Timing;
use trace::TraceKind;

const FANOUT: usize = 5;

/// The Push server
#[derive(Debug)]
pub struct Push<'a> {
    pub server: &'a Server,
    pub timing: Timing,
}

impl<'a> Push<'a> {
    /// Creates a new Push instance from a Server and Timing
    pub fn new(server: &'a Server, timing: Timing) -> Push {
        Push {
            server: server,
            timing: timing,
        }
    }

    /// Executes the Push thread. Gets a list of members to talk to that are not Confirmed; then
    /// proceeds to process the list in `FANOUT` sized chunks. If we finish sending the messages to
    /// all FANOUT targets faster than `Timing::GOSSIP_PERIOD_DEFAULT_MS`, we will block until we
    /// exceed that time.
    pub fn run(&mut self) {
        'send: loop {
            if self.server.pause.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(100));
                continue;
            }

            let mut check_list = self.server
                .member_list
                .check_list(self.server
                    .member
                    .read()
                    .expect("Member is poisoned")
                    .get_id());

            'fanout: loop {
                let mut thread_list = Vec::with_capacity(FANOUT);
                if check_list.len() == 0 {
                    break 'fanout;
                }
                let drain_length = if check_list.len() >= FANOUT {
                    FANOUT
                } else {
                    check_list.len()
                };
                let next_gossip = self.timing.gossip_timeout();
                for member in check_list.drain(0..drain_length) {
                    if self.server.check_blacklist(member.get_id()) {
                        debug!("Not sending rumors to {} - it is blacklisted",
                               member.get_id());
                        continue;
                    }

                    if self.server.member_list.pingable(&member) {
                        let rumors = self.server.rumor_list.rumors(member.get_id());
                        if rumors.len() > 0 {
                            let sc = self.server.clone();

                            let guard = match thread::Builder::new()
                                .name(String::from("push-worker"))
                                .spawn(move || {
                                    PushWorker::new(sc).send_rumors(member, rumors);
                                }) {
                                Ok(guard) => guard,
                                Err(e) => {
                                    error!("Could not spawn thread: {}", e);
                                    continue;
                                }
                            };
                            thread_list.push(guard);
                        }
                    }
                }
                let num_threads = thread_list.len();
                for guard in thread_list.drain(0..num_threads) {
                    match guard.join() {
                        Ok(()) => {}
                        Err(e) => println!("Push worker died: {:?}", e),
                    }
                }
                'gossip_period: loop {
                    if SteadyTime::now() > next_gossip {
                        break 'gossip_period;
                    } else {
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        }
    }
}

/// A worker thread for pushing messages to a target
struct PushWorker {
    pub server: Server,
}

impl PushWorker {
    /// Create a new PushWorker.
    pub fn new(server: Server) -> PushWorker {
        PushWorker { server: server }
    }

    /// Send the list of rumors to a given member. This method creates an outbound socket and then
    /// closes the connection as soon as we are done sending rumors. ZeroMQ may choose to keep the
    /// connection and socket open for 1 second longer - so it is possible, but unlikely, that this
    /// method can loose messages.
    fn send_rumors(&self, member: Member, rumors: RumorVec) {
        let mut socket = (**ZMQ_CONTEXT)
            .as_mut()
            .socket(zmq::PUSH)
            .expect("Failure to create the ZMQ push socket");
        socket.set_linger(1000)
            .expect("Failure to set the ZMQ push socket to not linger");
        socket.set_tcp_keepalive(0)
            .expect("Failure to set the ZMQ push socket to not use keepalive");
        socket.set_immediate(true).expect("Failure to set the ZMQ push socket to immediate");
        socket.set_sndhwm(1000).expect("Failure to set the ZMQ push socket hwm");
        socket.set_sndtimeo(500).expect("Failure to set the ZMQ send timeout");
        let to_addr = format!("{}:{}", member.get_address(), member.get_gossip_port());
        match socket.connect(&format!("tcp://{}", to_addr)) {
            Ok(()) => debug!("Connected push socket to {:?}", member),
            Err(e) => {
                println!("Cannot connect push socket to {:?}: {:?}", member, e);
                return;
            }
        }
        'rumorlist: for &(ref rumor, ref _heat) in rumors.iter() {
            let send_rumor = match &rumor.kind[..] {
                "member" => self.create_member_rumor(&rumor),
                k => {
                    println!("Unknown rumor type; add it to the push server {:?}", k);
                    continue 'rumorlist;
                }
            };
            let rumor_as_bytes = match send_rumor.write_to_bytes() {
                Ok(bytes) => bytes,
                Err(e) => {
                    println!("Could not write our own rumor to bytes; abandoning \
                                            sending rumor: {:?}",
                             e);
                    continue 'rumorlist;
                }
            };
            match socket.send(&rumor_as_bytes[..], 0) {
                Ok(()) => debug!("Sent rumor {:?} to {:?}", rumor, member),
                Err(e) => println!("Could not send rumor to {:?}; ZMQ said: {:?}", member, e),
            }
            trace_it!(GOSSIP: &self.server, TraceKind::SendRumor, member.get_id(), to_addr, &send_rumor);
        }
        self.server.rumor_list.update_heat(member.get_id(), &rumors);
    }

    /// Given a rumorkey, creates a protobuf rumor for sharing.
    fn create_member_rumor(&self, rumor_key: &RumorKey) -> ProtoRumor {
        let mut member: ProtoMember = ProtoMember::new();
        self.server.member_list.with_member(&rumor_key.key, |m| {
            // TODO: This should not stand
            member = m.unwrap().proto.clone();
        });
        let mut membership = ProtoMembership::new();
        membership.set_member(member);
        membership.set_health(self.server.member_list.health_of_by_id(&rumor_key.key).unwrap().into());
        let mut rumor = ProtoRumor::new();
        rumor.set_field_type(ProtoRumor_Type::Member);
        rumor.set_member(membership);
        rumor.set_from_id(String::from(self.server
            .member
            .read()
            .expect("Member lock is poisoned")
            .get_id()));
        rumor.set_from_address(format!("{}", self.server.gossip_addr()));
        rumor
    }
}
