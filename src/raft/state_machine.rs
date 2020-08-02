use bytes::Bytes;

// Result of executing an operation on the state machine.
pub type StateMachineResult = Result<(), String>;

// A state machine kept on every server in a raft cluster. The consensus
// implementation applies payloads once they are committed.
pub trait StateMachine {
    // Applies the supplied payload and incorporates it in the state of the
    // state machine.
    fn apply(&mut self, operation: &Bytes) -> StateMachineResult;

    // Creates a snapshot of the full current state in a format that can loaded
    // with a call to 'load_snapshot'.
    fn create_snapshot(&self) -> Bytes;

    // Loads a snapshot produced by a previous call to 'create_snapshot'. Any
    // state previously held in this instance is replaced by the contents of
    // the loaded snapshot.
    fn load_snapshot(&mut self, snapshot: &Bytes) -> StateMachineResult;
}
