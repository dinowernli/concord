use crate::raft::{StateMachine, StateMachineResult};
use bytes::Bytes;

// A fake implementation of the StateMachine trait for testing
// purposes.
pub struct FakeStateMachine {
    committed: i64,
    snapshots_loaded: i64,
}

impl FakeStateMachine {
    pub fn new() -> Self {
        FakeStateMachine {
            committed: 0,
            snapshots_loaded: 0,
        }
    }
}

impl StateMachine for FakeStateMachine {
    fn apply(&mut self, _operation: &Bytes) -> StateMachineResult {
        self.committed += 1;
        Ok(())
    }

    fn create_snapshot(&self) -> Bytes {
        Bytes::from(Vec::new())
    }

    fn load_snapshot(&mut self, _snapshot: &Bytes) -> StateMachineResult {
        self.snapshots_loaded += 1;
        Ok(())
    }
}
