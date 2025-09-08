use criterion::{black_box, criterion_group, criterion_main, Criterion};
use azolla_client::{FromJsonValue, ConversionError};
use serde_json::{json, Value};

fn bench_basic_conversions(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic_conversions");
    
    // Benchmark i32 conversion
    group.bench_function("i32_conversion", |b| {
        let value = json!(42);
        b.iter(|| {
            i32::try_from(black_box(value.clone())).unwrap()
        })
    });
    
    // Benchmark String conversion
    group.bench_function("string_conversion", |b| {
        let value = json!("hello world");
        b.iter(|| {
            String::try_from(black_box(value.clone())).unwrap()
        })
    });
    
    // Benchmark bool conversion
    group.bench_function("bool_conversion", |b| {
        let value = json!(true);
        b.iter(|| {
            bool::try_from(black_box(value.clone())).unwrap()
        })
    });
    
    group.finish();
}

fn bench_vec_conversions(c: &mut Criterion) {
    let mut group = c.benchmark_group("vec_conversions");
    
    // Small array
    group.bench_function("small_vec_i32", |b| {
        let value = json!([1, 2, 3, 4, 5]);
        b.iter(|| {
            Vec::<i32>::try_from(black_box(value.clone())).unwrap()
        })
    });
    
    // Large array
    group.bench_function("large_vec_i32", |b| {
        let large_array: Vec<i32> = (0..1000).collect();
        let value = json!(large_array);
        b.iter(|| {
            Vec::<i32>::try_from(black_box(value.clone())).unwrap()
        })
    });
    
    // Nested vec
    group.bench_function("nested_vec", |b| {
        let nested: Vec<Vec<i32>> = (0..10).map(|i| vec![i; 10]).collect();
        let value = json!(nested);
        b.iter(|| {
            Vec::<Vec<i32>>::try_from(black_box(value.clone())).unwrap()
        })
    });
    
    group.finish();
}

fn bench_option_conversions(c: &mut Criterion) {
    let mut group = c.benchmark_group("option_conversions");
    
    group.bench_function("option_some", |b| {
        let value = json!(42);
        b.iter(|| {
            Option::<i32>::try_from(black_box(value.clone())).unwrap()
        })
    });
    
    group.bench_function("option_none", |b| {
        let value = json!(null);
        b.iter(|| {
            Option::<i32>::try_from(black_box(value.clone())).unwrap()
        })
    });
    
    group.finish();
}

fn bench_error_cases(c: &mut Criterion) {
    let mut group = c.benchmark_group("error_cases");
    
    group.bench_function("type_mismatch_i32", |b| {
        let value = json!("not_a_number");
        b.iter(|| {
            let _: Result<i32, ConversionError> = i32::try_from(black_box(value.clone()));
        })
    });
    
    group.bench_function("type_mismatch_array", |b| {
        let value = json!("not_an_array");
        b.iter(|| {
            let _: Result<Vec<i32>, ConversionError> = Vec::<i32>::try_from(black_box(value.clone()));
        })
    });
    
    group.finish();
}

fn bench_complex_structures(c: &mut Criterion) {
    let mut group = c.benchmark_group("complex_structures");
    
    group.bench_function("deeply_nested", |b| {
        let complex = json!([
            [
                [Some(1), None, Some(3)],
                [Some(4), Some(5), None]
            ],
            [
                [None, Some(7), Some(8)],
                [Some(9), None, Some(11)]
            ]
        ]);
        b.iter(|| {
            Vec::<Vec<Vec<Option<i32>>>>::try_from(black_box(complex.clone())).unwrap()
        })
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_basic_conversions,
    bench_vec_conversions,
    bench_option_conversions,
    bench_error_cases,
    bench_complex_structures
);
criterion_main!(benches);