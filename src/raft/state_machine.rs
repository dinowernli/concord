use bytes::Bytes;

// A state machine kept on every server in a raft cluster. The consensus
// implementation applies payloads once they are committed.
pub trait StateMachine {
    // Applies the supplied payload and incorporates it in the state of the
    // state machine.
    fn apply(&mut self, payload: &Bytes);
}
