use pin_project::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::transmute;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct Scope<'env> {
    handle: tokio::runtime::Handle,
    chan: mpsc::Sender<()>,
    _marker: PhantomData<&'env mut &'env ()>,
}

#[pin_project]
pub struct ScopedJoinHandle<'scope, R> {
    #[pin]
    handle: tokio::task::JoinHandle<R>,
    _marker: PhantomData<&'scope ()>,
}

impl<'env, R> Future for ScopedJoinHandle<'env, R> {
    type Output = Result<R, tokio::task::JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.project().handle.poll(cx)
    }
}

impl<'env> Scope<'env> {
    pub fn spawn<'scope, F, R>(&'scope self, fut: F) -> ScopedJoinHandle<'scope, R>
    where
        F: Future<Output = R> + Send + 'env,
        R: Send + 'static, // TODO: weaken to 'env
    {
        let chan = self.chan.clone();
        let future_env: Pin<Box<dyn Future<Output = R> + Send + 'env>> = Box::pin(async move {
            // the cloned channel gets dropped at the end of the future
            let _chan = chan;
            fut.await
        });

        // SAFETY: scoped API ensures the spawned tasks will not outlive the parent scope
        let future_static: Pin<Box<dyn Future<Output = R> + Send + 'static>> =
            unsafe { transmute(future_env) };

        let handle = self.handle.spawn(future_static);

        ScopedJoinHandle {
            handle,
            _marker: PhantomData,
        }
    }
}

// TODO: if `Func` takes a reference to the scope, `scope.spawn` will generate a cryptic error
#[doc(hidden)]
pub async unsafe fn scope_impl<'env, Func, Fut, R>(handle: tokio::runtime::Handle, func: Func) -> R
where
    Func: FnOnce(Scope<'env>) -> Fut,
    Fut: Future<Output = R> + Send,
    R: Send,
{
    // we won't send data through this channel, so reserve the minimal buffer (buffer size must be
    // greater than 0).
    let (tx, mut rx) = mpsc::channel(1);
    let scope = Scope::<'env> {
        handle,
        chan: tx,
        _marker: PhantomData,
    };

    // TODO: `func` and the returned future can panic during the execution.
    // In that case, we need to cancel all the spawned subtasks forcibly, but we cannot cancel
    // spawned tasks from the outside of tokio.
    let result = func(scope).await;

    // yield the control until all spawned task finish(drop).
    assert!(rx.recv().await.is_none());

    result
}

#[macro_export]
macro_rules! scope {
    ($handle:expr, $func:expr) => {{
        unsafe { crate::scope_impl($handle, $func) }.await
    }};
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use tokio::time::delay_for;

    #[test]
    fn test_scoped() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();

        rt.block_on(async {
            {
                let local = String::from("hello world");
                let local = &local;

                scope!(handle, |scope| {
                    // TODO: without this `move`, we get a compilation error. why?
                    async move {
                        // this spawned subtask will continue running after the scoped task
                        // finished, but `scope!` will wait until this task completes.
                        scope.spawn(async move {
                            delay_for(Duration::from_millis(500)).await;
                            println!("spanwed task is done: {}", local);
                        });

                        // since spawned tasks can outlive the scoped task, they cannot have
                        // references to the scoped task's stack
                        /*
                        let evil = String::from("may dangle");
                        scope.spawn(async {
                            delay_for(Duration::from_millis(200)).await;
                            println!("spanwed task cannot access evil: {}", evil);
                        });
                        */

                        let handle = scope.spawn(async {
                            println!("another spawned task");
                        });
                        handle.await.unwrap(); // you can await the returned handle

                        delay_for(Duration::from_millis(100)).await;
                        println!("scoped task is done: {}", local);
                    }
                });

                delay_for(Duration::from_millis(110)).await;
                println!("local can be used here: {}", local);
            }

            println!("local is freed");
            delay_for(Duration::from_millis(600)).await;
        });
    }
}
