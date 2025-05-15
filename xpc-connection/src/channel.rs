use futures::{
    channel::mpsc::{UnboundedReceiver, UnboundedSender},
    Stream,
};
use std::{ops::Deref, sync::{atomic::{AtomicBool, Ordering}, Arc}};
use std::{pin::Pin, task::Poll};
use xpc_connection_sys::{
    xpc_connection_send_message,  xpc_connection_t, xpc_object_t, xpc_release,
};

use crate::{cancel_and_wait_for_event_handler, message_to_xpc_object, Message, MessageError};

/// A wrapper around [`xpc_connection_t`] which is shared by a [`XpcSender`], [`XpcReceiver`] pair.
#[derive(Debug)]
pub(crate) struct DuplexConnection {
    connection: xpc_connection_t,
    event_handler_is_running: AtomicBool,
}

// thread safety notes:
// - Arc and AtomicBool are explicitly thread safe
// - the internal raw pointer xpc_connection_t is safe to share between threads
// - xpc_connection_send_message is probably thread-safe, as it is ["safe to call from multiple GCD queues"](https://developer.apple.com/documentation/xpc/xpc_connection_send_message(_:_:)?language=objc)
// - all other operations on xpc_connection_t are called from create or Drop functions which are exclusive
unsafe impl Send for DuplexConnection {}
unsafe impl Sync for DuplexConnection {}

impl DuplexConnection {
    pub(crate) fn new(connection: xpc_connection_t, event_handler_is_running: bool) -> Self {
        Self { connection, event_handler_is_running: AtomicBool::new(event_handler_is_running) }
    }
}

impl Deref for DuplexConnection {
    type Target = xpc_connection_t;
    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}
impl Drop for DuplexConnection {
    fn drop(&mut self) {
        if self.event_handler_is_running.load(Ordering::Acquire) {
            cancel_and_wait_for_event_handler(self.connection);
        }

        unsafe { xpc_release(self.connection as xpc_object_t) };
    }
}

#[derive(Debug)]
pub struct XpcReceiver {
    connection: Arc<DuplexConnection>,
    receiver: UnboundedReceiver<Message>,
    _sender: UnboundedSender<Message>,
}
unsafe impl Send for XpcReceiver {}
impl XpcReceiver {
    pub(crate) fn new(connection: Arc<DuplexConnection>, receiver: UnboundedReceiver<Message>, sender: UnboundedSender<Message>) -> Self {
        Self { connection, receiver, _sender: sender }
    }
}

impl Stream for XpcReceiver {
    type Item = Message;

    /// `Poll::Ready(None)` returned in place of `MessageError::ConnectionInvalid`
    /// as it's not recoverable. `MessageError::ConnectionInterrupted` should
    /// be treated as recoverable.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match Stream::poll_next(Pin::new(&mut self.receiver), cx) {
            Poll::Ready(Some(Message::Error(MessageError::ConnectionInvalid))) => {
                self.connection.event_handler_is_running.store(false, Ordering::Release);
                Poll::Ready(None)
            }
            v => v,
        }
    }
}

#[derive(Debug)]
pub struct XpcSender {
    connection: Arc<DuplexConnection>,
}
unsafe impl Send for XpcSender {}
impl XpcSender {
    pub(crate) fn new(connection: Arc<DuplexConnection>) -> Self {
        Self { connection }
    }
    
    /// The connection is established on the first call to `send_message`. You
    /// may receive an error relating to an invalid mach port name or a variety
    /// of other issues.
    pub fn send_message(&self, message: Message) {
        let xpc_object = message_to_xpc_object(message);
        unsafe {
            xpc_connection_send_message(**self.connection, xpc_object);
            xpc_release(xpc_object);
        }
    }
}