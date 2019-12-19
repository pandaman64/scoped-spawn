use crossbeam::sync::WaitGroup;
use pin_project::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::transmute;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Clone)]
pub struct Scope<'env> {
    handle: tokio::runtime::Handle,
    wait_group: WaitGroup,
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
        F: Future<Output = R> + Send + 'scope,
        R: Send + 'static, // TODO: weaken to 'env
    {
        let wg = self.wait_group.clone();
        let future_env: Pin<Box<dyn Future<Output = R> + Send + 'scope>> = Box::pin(async move {
            let _wg = wg;
            fut.await
        });

        // SAFETY: scoped API ensures the spawned future will not outlive the parent scope
        let future_static: Pin<Box<dyn Future<Output = R> + Send + 'static>> =
            unsafe { transmute(future_env) };

        let handle = self.handle.spawn(future_static);

        ScopedJoinHandle {
            handle,
            _marker: PhantomData,
        }
    }
}

#[doc(hidden)]
pub async fn scope_impl<'env, Func, Fut, R>(handle: tokio::runtime::Handle, func: Func) -> R
where
    Func: FnOnce(Scope<'env>) -> Fut,
    Fut: Future<Output = R> + Send + 'env,
    R: Send + 'env,
{
    let wg = WaitGroup::new();
    let scope = Scope::<'env> {
        handle,
        wait_group: wg.clone(),
        _marker: PhantomData,
    };

    // the following TODOs needs access to untyped JoinHandles to cancel/await the spawned subtasks.

    // TODO: `func` and the returned future can panic during the execution.
    // In that case, we need to cancel all the spawned subtasks forcibly.
    let result = func(scope).await;

    // TODO: instead of blocking the thread, we should await the spawned subtasks.
    wg.wait();

    result
}

#[macro_export]
macro_rules! scope {
    ($handle:expr, $func:expr) => {{
        crate::scope_impl($handle, $func).await
    }};
}

#[cfg(test)]
mod test {
    #[test]
    fn test_scoped() {
        use super::scope;
        use std::time::Duration;
        use tokio::time::delay_for;

        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();

        rt.block_on(async {
            {
                let local = String::from("hello_world");
                let local = &local;

                scope!(handle, |scope| {
                    // TODO: removing this `move` result in an error,
                    // so we need to take a reference to `local` and move the reference.
                    async move {
                        // this spawned subtask will continue running after the scoped task
                        // finished, but `scope!` will wait until this task completes.
                        scope.spawn(async {
                            delay_for(Duration::from_millis(500)).await;
                            println!("spanwed task is done: {}", local);
                        });

                        let danger = String::from("dangerous");
                        scope.spawn(async {
                            delay_for(Duration::from_millis(400)).await;
                            println!("dangerous task is done: {}", danger);
                        });

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
