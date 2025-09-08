use criterion::{black_box, criterion_group, criterion_main, Criterion};
use azolla_client::{azolla_task, Task, TaskError, TaskResult};
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;

// Define benchmark tasks using the proc macro
#[azolla_task]
async fn simple_add(a: i32, b: i32) -> Result<Value, TaskError> {
    Ok(json!({"result": a + b}))
}

#[azolla_task]
async fn string_process(input: String, repeat: u32) -> Result<Value, TaskError> {
    let result = input.repeat(repeat as usize);
    Ok(json!({"processed": result, "length": result.len()}))
}

#[azolla_task]
async fn array_sum(numbers: Vec<i32>) -> Result<Value, TaskError> {
    let sum: i32 = numbers.iter().sum();
    let avg = sum as f64 / numbers.len() as f64;
    Ok(json!({"sum": sum, "average": avg, "count": numbers.len()}))
}

#[azolla_task]
async fn complex_processing(
    data: Vec<String>,
    multiplier: f64,
    options: Option<Vec<bool>>
) -> Result<Value, TaskError> {
    let processed_data: Vec<_> = data.iter()
        .map(|s| s.to_uppercase())
        .collect();
    
    let total_length: usize = processed_data.iter()
        .map(|s| s.len())
        .sum();
    
    let final_result = (total_length as f64 * multiplier) as u32;
    
    Ok(json!({
        "processed_data": processed_data,
        "total_length": total_length,
        "multiplier": multiplier,
        "final_result": final_result,
        "options_count": options.as_ref().map(|v| v.len()).unwrap_or(0)
    }))
}

// Manual task implementation for comparison
struct ManualAddTask;

impl Task for ManualAddTask {
    fn name(&self) -> &'static str {
        "manual_add"
    }
    
    fn execute(&self, args: Vec<Value>) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move {
            if args.len() != 2 {
                return Err(TaskError::invalid_args("Expected 2 arguments"));
            }
            
            let a = args[0].as_i64().ok_or_else(|| TaskError::invalid_args("First arg not a number"))? as i32;
            let b = args[1].as_i64().ok_or_else(|| TaskError::invalid_args("Second arg not a number"))? as i32;
            
            Ok(json!({"result": a + b}))
        })
    }
}

fn bench_simple_task_execution(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("simple_task_execution");
    
    group.bench_function("proc_macro_add", |b| {
        let task = SimpleAddTask;
        let args = vec![json!(10), json!(20)];
        b.to_async(&rt).iter(|| async {
            task.execute(black_box(args.clone())).await.unwrap()
        })
    });
    
    group.bench_function("manual_add", |b| {
        let task = ManualAddTask;
        let args = vec![json!(10), json!(20)];
        b.to_async(&rt).iter(|| async {
            task.execute(black_box(args.clone())).await.unwrap()
        })
    });
    
    group.finish();
}

fn bench_string_processing(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("string_processing");
    
    group.bench_function("small_string", |b| {
        let task = StringProcessTask;
        let args = vec![json!("hello"), json!(5)];
        b.to_async(&rt).iter(|| async {
            task.execute(black_box(args.clone())).await.unwrap()
        })
    });
    
    group.bench_function("large_string", |b| {
        let task = StringProcessTask;
        let large_string = "x".repeat(1000);
        let args = vec![json!(large_string), json!(10)];
        b.to_async(&rt).iter(|| async {
            task.execute(black_box(args.clone())).await.unwrap()
        })
    });
    
    group.finish();
}

fn bench_array_processing(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("array_processing");
    
    group.bench_function("small_array", |b| {
        let task = ArraySumTask;
        let args = vec![json!([1, 2, 3, 4, 5])];
        b.to_async(&rt).iter(|| async {
            task.execute(black_box(args.clone())).await.unwrap()
        })
    });
    
    group.bench_function("large_array", |b| {
        let task = ArraySumTask;
        let large_array: Vec<i32> = (1..=1000).collect();
        let args = vec![json!(large_array)];
        b.to_async(&rt).iter(|| async {
            task.execute(black_box(args.clone())).await.unwrap()
        })
    });
    
    group.finish();
}

fn bench_complex_task(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("complex_task");
    
    group.bench_function("with_options", |b| {
        let task = ComplexProcessingTask;
        let args = vec![
            json!(["hello", "world", "foo", "bar"]),
            json!(2.5),
            json!([true, false, true, false, true])
        ];
        b.to_async(&rt).iter(|| async {
            task.execute(black_box(args.clone())).await.unwrap()
        })
    });
    
    group.bench_function("without_options", |b| {
        let task = ComplexProcessingTask;
        let args = vec![
            json!(["hello", "world", "foo", "bar"]),
            json!(2.5),
            json!(null)
        ];
        b.to_async(&rt).iter(|| async {
            task.execute(black_box(args.clone())).await.unwrap()
        })
    });
    
    group.finish();
}

fn bench_argument_parsing(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("argument_parsing");
    
    group.bench_function("simple_args", |b| {
        let task = SimpleAddTask;
        let args = vec![json!(42), json!(58)];
        b.to_async(&rt).iter(|| async {
            task.execute(black_box(args.clone())).await.unwrap()
        })
    });
    
    group.bench_function("complex_args", |b| {
        let task = ComplexProcessingTask;
        let complex_data: Vec<String> = (0..50).map(|i| format!("item_{i}")).collect();
        let args = vec![
            json!(complex_data),
            json!(1.234567),
            json!((0..25).map(|i| i % 2 == 0).collect::<Vec<bool>>())
        ];
        b.to_async(&rt).iter(|| async {
            task.execute(black_box(args.clone())).await.unwrap()
        })
    });
    
    group.finish();
}

fn bench_error_handling(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("error_handling");
    
    group.bench_function("invalid_args_count", |b| {
        let task = SimpleAddTask;
        let args = vec![json!(42)]; // Missing second argument
        b.to_async(&rt).iter(|| async {
            let _ = task.execute(black_box(args.clone())).await;
        })
    });
    
    group.bench_function("invalid_arg_type", |b| {
        let task = SimpleAddTask;
        let args = vec![json!("not_a_number"), json!(42)];
        b.to_async(&rt).iter(|| async {
            let _ = task.execute(black_box(args.clone())).await;
        })
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_simple_task_execution,
    bench_string_processing,
    bench_array_processing,
    bench_complex_task,
    bench_argument_parsing,
    bench_error_handling
);
criterion_main!(benches);