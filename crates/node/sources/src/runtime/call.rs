use crate::{RuntimeConfig, RuntimeLoader, RuntimeLoaderError};
use kona_protocol::BlockInfo;
use pin_project::pin_project;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// A wrapper around a runtime loader call that implements Future and allows setting block info.
#[pin_project]
pub struct RuntimeCall {
    #[pin]
    loader: RuntimeLoader,
    block_info: Option<BlockInfo>,
    #[pin]
    state: RuntimeCallState,
}

impl core::fmt::Debug for RuntimeCall {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RuntimeCall")
            .field("loader", &self.loader)
            .field("block_info", &self.block_info)
            .field("state", &self.state)
            .finish()
    }
}

#[pin_project(project = RuntimeCallStateProj)]
enum RuntimeCallState {
    Initial,
    LoadingLatest(#[pin] Pin<Box<dyn Future<Output = Result<RuntimeConfig, RuntimeLoaderError>>>>),
    LoadingWithBlockInfo(
        #[pin] Pin<Box<dyn Future<Output = Result<RuntimeConfig, RuntimeLoaderError>>>>,
    ),
    #[allow(unused)]
    Complete(Result<RuntimeConfig, RuntimeLoaderError>),
}

impl core::fmt::Debug for RuntimeCallState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Initial => f.debug_tuple("Initial").finish(),
            Self::LoadingLatest(_) => f.debug_tuple("LoadingLatest").finish(),
            Self::LoadingWithBlockInfo(_) => f.debug_tuple("LoadingWithBlockInfo").finish(),
            Self::Complete(_) => f.debug_tuple("Complete").finish(),
        }
    }
}

impl RuntimeCall {
    /// Creates a new RuntimeCall with the given loader.
    pub const fn new(loader: RuntimeLoader) -> Self {
        Self { loader, block_info: None, state: RuntimeCallState::Initial }
    }

    /// Sets the block info to use for loading.
    pub const fn block_info(mut self, block_info: BlockInfo) -> Self {
        self.block_info = Some(block_info);
        self
    }
}

impl Future for RuntimeCall {
    type Output = Result<RuntimeConfig, RuntimeLoaderError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let state = this.state.as_mut().project();

        match state {
            RuntimeCallStateProj::Initial => {
                if let Some(block_info) = this.block_info.take() {
                    let mut loader = this.loader.clone();
                    let fut = Box::pin(async move { loader.load_internal(block_info).await });
                    this.state.set(RuntimeCallState::LoadingWithBlockInfo(fut));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    let mut loader = this.loader.clone();
                    let fut = Box::pin(async move { loader.load_latest().await });
                    this.state.set(RuntimeCallState::LoadingLatest(fut));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            RuntimeCallStateProj::LoadingLatest(fut) => fut.poll(cx),
            RuntimeCallStateProj::LoadingWithBlockInfo(fut) => fut.poll(cx),
            RuntimeCallStateProj::Complete(result) => Poll::Ready(result.clone()),
        }
    }
}
