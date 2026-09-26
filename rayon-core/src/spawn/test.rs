use crate::scope;
use std::any::Any;
use std::sync::Mutex;
use std::sync::mpsc::channel;

use super::{spawn, spawn_fifo};
use crate::ThreadPoolBuilder;

#[test]
#[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
fn spawn_then_join_in_worker() {
    let (tx, rx) = channel();
    scope(move |_| {
        spawn(move || tx.send(22).unwrap());
    });
    assert_eq!(22, rx.recv().unwrap());
}

#[test]
#[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
fn spawn_then_join_outside_worker() {
    let (tx, rx) = channel();
    spawn(move || tx.send(22).unwrap());
    assert_eq!(22, rx.recv().unwrap());
}

#[test]
#[cfg_attr(not(panic = "unwind"), ignore)]
fn panic_fwd() {
    let (tx, rx) = channel();

    let tx = Mutex::new(tx);
    let panic_handler = move |err: Box<dyn Any + Send>| {
        let tx = tx.lock().unwrap();
        if let Some(&msg) = err.downcast_ref::<&str>() {
            if msg == "Hello, world!" {
                tx.send(1).unwrap();
            } else {
                tx.send(2).unwrap();
            }
        } else {
            tx.send(3).unwrap();
        }
    };

    let builder = ThreadPoolBuilder::new().panic_handler(panic_handler);

    builder
        .build()
        .unwrap()
        .spawn(move || panic!("Hello, world!"));

    assert_eq!(1, rx.recv().unwrap());
}

/// Test what happens when the thread pool is dropped but there are
/// still active asynchronous tasks. We expect the thread pool to stay
/// alive and executing until those threads are complete.
#[test]
#[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
fn termination_while_things_are_executing() {
    let (tx0, rx0) = channel();
    let (tx1, rx1) = channel();

    // Create a thread pool and spawn some code in it, but then drop
    // our reference to it.
    {
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        thread_pool.spawn(move || {
            let data = rx0.recv().unwrap();

            // At this point, we know the "main" reference to the
            // `ThreadPool` has been dropped, but there are still
            // active threads. Launch one more.
            spawn(move || {
                tx1.send(data).unwrap();
            });
        });
    }

    tx0.send(22).unwrap();
    let v = rx1.recv().unwrap();
    assert_eq!(v, 22);
}

#[test]
#[cfg_attr(not(panic = "unwind"), ignore)]
fn custom_panic_handler_and_spawn() {
    let (tx, rx) = channel();

    // Create a parallel closure that will send panics on the
    // channel; since the closure is potentially executed in parallel
    // with itself, we have to wrap `tx` in a mutex.
    let tx = Mutex::new(tx);
    let panic_handler = move |e: Box<dyn Any + Send>| {
        tx.lock().unwrap().send(e).unwrap();
    };

    // Execute an async that will panic.
    let builder = ThreadPoolBuilder::new().panic_handler(panic_handler);
    builder.build().unwrap().spawn(move || {
        panic!("Hello, world!");
    });

    // Check that we got back the panic we expected.
    let error = rx.recv().unwrap();
    if let Some(&msg) = error.downcast_ref::<&str>() {
        assert_eq!(msg, "Hello, world!");
    } else {
        panic!("did not receive a string from panic handler");
    }
}

#[test]
#[cfg_attr(not(panic = "unwind"), ignore)]
fn custom_panic_handler_and_nested_spawn() {
    let (tx, rx) = channel();

    // Create a parallel closure that will send panics on the
    // channel; since the closure is potentially executed in parallel
    // with itself, we have to wrap `tx` in a mutex.
    let tx = Mutex::new(tx);
    let panic_handler = move |e| {
        tx.lock().unwrap().send(e).unwrap();
    };

    // Execute an async that will (eventually) panic.
    const PANICS: usize = 3;
    let builder = ThreadPoolBuilder::new().panic_handler(panic_handler);
    builder.build().unwrap().spawn(move || {
        // launch 3 nested spawn-asyncs; these should be in the same
        // thread pool and hence inherit the same panic handler
        for _ in 0..PANICS {
            spawn(move || {
                panic!("Hello, world!");
            });
        }
    });

    // Check that we get back the panics we expected.
    for _ in 0..PANICS {
        let error = rx.recv().unwrap();
        if let Some(&msg) = error.downcast_ref::<&str>() {
            assert_eq!(msg, "Hello, world!");
        } else {
            panic!("did not receive a string from panic handler");
        }
    }
}

macro_rules! test_order {
    ($outer_spawn:ident, $inner_spawn:ident) => {{
        let builder = ThreadPoolBuilder::new().num_threads(1);
        let pool = builder.build().unwrap();
        let (tx, rx) = channel();
        pool.install(move || {
            for i in 0..10 {
                let tx = tx.clone();
                $outer_spawn(move || {
                    for j in 0..10 {
                        let tx = tx.clone();
                        $inner_spawn(move || {
                            tx.send(i * 10 + j).unwrap();
                        });
                    }
                });
            }
        });
        rx.iter().collect::<Vec<i32>>()
    }};
}

#[test]
#[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
fn lifo_order() {
    // In the absence of stealing, `spawn()` jobs on a thread will run in LIFO order.
    let vec = test_order!(spawn, spawn);
    let expected: Vec<i32> = (0..100).rev().collect(); // LIFO -> reversed
    assert_eq!(vec, expected);
}

#[test]
#[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
fn fifo_order() {
    // In the absence of stealing, `spawn_fifo()` jobs on a thread will run in FIFO order.
    let vec = test_order!(spawn_fifo, spawn_fifo);
    let expected: Vec<i32> = (0..100).collect(); // FIFO -> natural order
    assert_eq!(vec, expected);
}

#[test]
#[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
fn lifo_fifo_order() {
    // LIFO on the outside, FIFO on the inside
    let vec = test_order!(spawn, spawn_fifo);
    let expected: Vec<i32> = (0..10)
        .rev()
        .flat_map(|i| (0..10).map(move |j| i * 10 + j))
        .collect();
    assert_eq!(vec, expected);
}

#[test]
#[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
fn fifo_lifo_order() {
    // FIFO on the outside, LIFO on the inside
    let vec = test_order!(spawn_fifo, spawn);
    let expected: Vec<i32> = (0..10)
        .flat_map(|i| (0..10).rev().map(move |j| i * 10 + j))
        .collect();
    assert_eq!(vec, expected);
}

macro_rules! spawn_send {
    ($spawn:ident, $tx:ident, $i:expr) => {{
        let tx = $tx.clone();
        $spawn(move || tx.send($i).unwrap());
    }};
}

/// Test mixed spawns pushing a series of numbers, interleaved such
/// such that negative values are using the second kind of spawn.
macro_rules! test_mixed_order {
    ($pos_spawn:ident, $neg_spawn:ident) => {{
        let builder = ThreadPoolBuilder::new().num_threads(1);
        let pool = builder.build().unwrap();
        let (tx, rx) = channel();
        pool.install(move || {
            spawn_send!($pos_spawn, tx, 0);
            spawn_send!($neg_spawn, tx, -1);
            spawn_send!($pos_spawn, tx, 1);
            spawn_send!($neg_spawn, tx, -2);
            spawn_send!($pos_spawn, tx, 2);
            spawn_send!($neg_spawn, tx, -3);
            spawn_send!($pos_spawn, tx, 3);
        });
        rx.iter().collect::<Vec<i32>>()
    }};
}

#[test]
#[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
fn mixed_lifo_fifo_order() {
    let vec = test_mixed_order!(spawn, spawn_fifo);
    let expected = vec![3, -1, 2, -2, 1, -3, 0];
    assert_eq!(vec, expected);
}

#[test]
#[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
fn mixed_fifo_lifo_order() {
    let vec = test_mixed_order!(spawn_fifo, spawn);
    let expected = vec![0, -3, 1, -2, 2, -1, 3];
    assert_eq!(vec, expected);
}

mod fallback {
    use super::{spawn, spawn_fifo};
    use crate::{ThreadPoolBuilder, Yield, join, spawn_broadcast, yield_now};
    use std::cell::Cell;
    use std::sync::Once;
    use std::sync::mpsc::channel;

    // The hook is global, but it runs synchronously on the spawning thread, so each
    // test's fallback thread can count wakes in its own thread-local.
    thread_local!(static WAKES: Cell<usize> = const { Cell::new(0) });

    fn wakes() -> usize {
        WAKES.get()
    }

    fn take_wakes() -> usize {
        WAKES.replace(0)
    }

    /// The fallback registry permanently takes over its thread, so run `f` on a fresh one.
    fn in_fallback_registry(f: impl FnOnce() + Send + 'static) {
        static HOOK: Once = Once::new();
        HOOK.call_once(|| {
            crate::set_fallback_wake_hook(|| WAKES.set(WAKES.get() + 1)).unwrap();
            let err = crate::set_fallback_wake_hook(|| {}).unwrap_err();
            assert_eq!(format!("{err:?}"), "FallbackWakeHookError(_)");
            err.into_inner()();
        });
        std::thread::spawn(|| {
            let builder = ThreadPoolBuilder::new().num_threads(1).use_current_thread();
            let _registry = crate::registry::Registry::new_fallback(builder).unwrap();
            f();
        })
        .join()
        .unwrap();
    }

    /// A host loop turn: run at most `budget` jobs, returning how many ran.
    fn drive(budget: usize) -> usize {
        (0..budget)
            .take_while(|_| yield_now() == Some(Yield::Executed))
            .count()
    }

    #[test]
    #[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
    fn spawn_only_queues() {
        in_fallback_registry(|| {
            let (tx, rx) = channel();
            spawn(move || tx.send(1).unwrap());
            assert!(rx.try_recv().is_err(), "spawn must not run inline");
            assert_eq!(wakes(), 1);
            assert_eq!(yield_now(), Some(Yield::Executed));
            assert_eq!(rx.try_recv(), Ok(1));
            assert_eq!(yield_now(), Some(Yield::Idle));
        });
    }

    #[test]
    #[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
    fn wake_coalesces_and_rearms() {
        in_fallback_registry(|| {
            spawn(|| {});
            spawn(|| {});
            spawn_fifo(|| {});
            assert_eq!(take_wakes(), 1, "one wake per idle-to-pending transition");

            // A single drive consumes the wake, even with jobs still queued.
            assert_eq!(yield_now(), Some(Yield::Executed));
            spawn(|| {});
            assert_eq!(take_wakes(), 1);

            // A drive that runs a job which itself spawns must re-arm.
            drive(usize::MAX);
            take_wakes();
            spawn(|| spawn(|| {}));
            assert_eq!(take_wakes(), 1);
            assert_eq!(yield_now(), Some(Yield::Executed));
            assert_eq!(wakes(), 1, "nested spawn re-armed the wake");
            drive(usize::MAX);
            take_wakes();

            // yield_local also consumes.
            spawn(|| {});
            take_wakes();
            assert_eq!(crate::yield_local(), Some(Yield::Executed));
            spawn(|| {});
            assert_eq!(take_wakes(), 1);
            drive(usize::MAX);
        });
    }

    #[test]
    #[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
    fn host_loop_runs_all_spawn_kinds() {
        in_fallback_registry(|| {
            let (tx, rx) = channel();
            spawn({
                let tx = tx.clone();
                move || tx.send("spawn").unwrap()
            });
            spawn_fifo({
                let tx = tx.clone();
                move || tx.send("spawn_fifo").unwrap()
            });
            spawn_broadcast(move |ctx| {
                assert_eq!((ctx.index(), ctx.num_threads()), (0, 1));
                tx.send("spawn_broadcast").unwrap()
            });
            assert!(rx.try_recv().is_err());

            let mut turns = 0;
            while take_wakes() > 0 {
                turns += 1;
                drive(usize::MAX);
            }
            assert_eq!(turns, 1);
            let mut ran: Vec<_> = rx.try_iter().collect();
            ran.sort();
            assert_eq!(ran, ["spawn", "spawn_broadcast", "spawn_fifo"]);
        });
    }

    #[test]
    #[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
    fn nested_spawn_runs_after_parent() {
        in_fallback_registry(|| {
            let (tx, rx) = channel();
            spawn({
                let tx = tx.clone();
                move || {
                    tx.send("outer start").unwrap();
                    spawn({
                        let tx = tx.clone();
                        move || tx.send("inner").unwrap()
                    });
                    tx.send("outer end").unwrap();
                }
            });
            drive(usize::MAX);
            let order: Vec<_> = rx.try_iter().collect();
            assert_eq!(order, ["outer start", "outer end", "inner"]);
        });
    }

    #[test]
    #[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
    fn spawn_chain_with_bounded_turns() {
        in_fallback_registry(|| {
            const DEPTH: usize = 100_000;
            const BUDGET: usize = 64;
            let (tx, rx) = channel();
            fn step(n: usize, tx: std::sync::mpsc::Sender<()>) {
                if n == 0 {
                    tx.send(()).unwrap();
                } else {
                    spawn(move || step(n - 1, tx));
                }
            }
            step(DEPTH, tx);

            // Each turn runs at most BUDGET jobs; the chain re-wakes itself so the
            // host keeps scheduling turns until it goes quiet.
            let mut turns = 0;
            while take_wakes() > 0 {
                turns += 1;
                assert!(drive(BUDGET) <= BUDGET);
            }
            assert_eq!(rx.try_recv(), Ok(()));
            // Jobs run in the final turn still wake, so the host takes one idle turn to notice.
            assert_eq!(turns, DEPTH.div_ceil(BUDGET) + 1);
            assert_eq!(yield_now(), Some(Yield::Idle));
        });
    }

    #[test]
    #[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
    fn spawn_inside_join_runs_when_join_blocks() {
        in_fallback_registry(|| {
            let (tx, rx) = channel();
            let (a, b) = join(
                {
                    let tx = tx.clone();
                    move || {
                        spawn(move || tx.send("spawned").unwrap());
                        "a"
                    }
                },
                || "b",
            );
            assert_eq!((a, b), ("a", "b"));
            assert_eq!(rx.try_recv(), Ok("spawned"));
            // join's wait_until drained the spawn, so a later drive is idle and the
            // next spawn wakes again.
            take_wakes();
            assert_eq!(yield_now(), Some(Yield::Idle));
            spawn(|| {});
            assert_eq!(take_wakes(), 1);
            drive(usize::MAX);
        });
    }
}
