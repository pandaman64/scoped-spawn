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

pub async fn scope<'env, Func, Fut, R>(handle: tokio::runtime::Handle, func: Func) -> R
where
    Func: FnOnce(Scope<'env>) -> Fut,
    Fut: Future<Output = R> + Send + 'env,
    R: Send + 'env,
{
    let wg = WaitGroup::new();
    let scope = Scope::<'env> {
        handle: handle.clone(),
        wait_group: wg.clone(),
        _marker: PhantomData,
    };

    let result = func(scope.clone()).await;

    drop(scope.wait_group);
    wg.wait();

    result
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

                let mut f = Box::pin(scope(handle, |scope| {
                    async move {
                        scope.spawn(async {
                            delay_for(Duration::from_millis(500)).await;
                            println!("spanwed task is done: {}", local);
                        });
                        delay_for(Duration::from_millis(100)).await;
                        println!("main task is done: {}", local);
                    }
                }));
                println!("{:?}", futures::poll!(f.as_mut()));
                delay_for(Duration::from_millis(110)).await;
                std::mem::forget(f);
                println!("local can be used here: {}", local);
            }
            println!("local is freed");
            delay_for(Duration::from_millis(600)).await;
        });
    }
}
