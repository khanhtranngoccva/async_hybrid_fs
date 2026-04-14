macro_rules! uring_cancel_impl {
    ($item:expr) => {{
        if $item.ack_tx.is_some() {
            return None;
        }
        if $item.result_tx.is_some() {
            return None;
        }
        if $item.completion_state.is_none() {
            // There is nothing to cancel, the operation is already done or cancelled
            return None;
        }
        let cancellation_id = {
            if $item.cancellation.is_none() {
                let id = $item
                    .ack_rx
                    .take()
                    .expect("ack_rx must be Some")
                    .recv()
                    .expect("ack_rx must be received to avoid a dangling pointer issue");
                $item.cancellation = Some(id);
            }
            $item.cancellation.unwrap()
        };
        let res = $item.client.cancel_uring(cancellation_id);
        $item.cancel_done = true;
        match res {
            Ok(_) => {
                // Await the future to ensure that the operation completely ends.
                // The result can be ignored because it is always ECANCELED
                let _ = $item._completion()?.await;
                None
            }
            // Assertion: the operation is done. However, the completion thread has not sent the completion event yet for some reason, so we still need to wait.
            Err(e) if e.raw_os_error() == Some(libc::ENOENT) => Some($item._completion()?.await),
            Err(e) if e.raw_os_error() == Some(libc::EALREADY) => Some($item._completion()?.await),
            // Only happens in case of a logic error
            Err(e) => panic!("failed to cancel operation: {}", e),
        }
    }};
}

pub(crate) use uring_cancel_impl;
