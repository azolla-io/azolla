use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use rand::prelude::*;
use serde_json::json;
use tokio::time::timeout;
use tonic::transport::Channel;

use azolla::orchestrator::retry_policy::{RetryPolicy as InternalRetryPolicy, WaitStrategy};
use azolla::proto::common::RetryPolicy as ProtoRetryPolicy;
use azolla::proto::orchestrator;
use orchestrator::client_service_client::ClientServiceClient;
use orchestrator::*;

#[derive(Parser)]
#[command(name = "benchmark")]
#[command(about = "Benchmark tool for Azolla create_task gRPC calls")]
struct Args {
    /// Server address
    #[arg(short, long, default_value = "http://[::1]:52710")]
    server: String,

    /// Total number of requests
    #[arg(short = 'n', long, default_value = "10000")]
    requests: usize,

    /// Number of concurrent connections
    #[arg(short, long, default_value = "10")]
    concurrency: usize,

    /// Number of domains for zipfian distribution
    #[arg(short, long, default_value = "10")]
    domains: usize,

    /// Task name prefix
    #[arg(long, default_value = "bench_task")]
    task_prefix: String,
}

struct ZipfianDistribution {
    domains: Vec<String>,
    cumulative_weights: Vec<f64>,
}

impl ZipfianDistribution {
    fn new(num_domains: usize) -> Self {
        let domains: Vec<String> = (0..num_domains).map(|i| format!("domain_{i}")).collect();

        // Generate zipfian weights: weight_i = 1/(i+1)
        let weights: Vec<f64> = (0..num_domains).map(|i| 1.0 / (i + 1) as f64).collect();

        // Normalize weights to sum to 1.0
        let total_weight: f64 = weights.iter().sum();
        let normalized_weights: Vec<f64> = weights.iter().map(|w| w / total_weight).collect();

        // Create cumulative weights for sampling
        let mut cumulative_weights = Vec::new();
        let mut cumsum = 0.0;
        for weight in &normalized_weights {
            cumsum += weight;
            cumulative_weights.push(cumsum);
        }

        Self {
            domains,
            cumulative_weights,
        }
    }

    fn select_domain(&self, rng: &mut impl Rng) -> &str {
        let rand_val: f64 = rng.gen();

        for (i, &cum_weight) in self.cumulative_weights.iter().enumerate() {
            if rand_val <= cum_weight {
                return &self.domains[i];
            }
        }

        // Fallback to last domain (should not happen with proper cumulative weights)
        &self.domains[self.domains.len() - 1]
    }
}

struct BenchmarkStats {
    total_requests: usize,
    successful_requests: usize,
    failed_requests: usize,
    total_duration: Duration,
    latencies: Vec<Duration>,
    domain_counts: HashMap<String, usize>,
}

impl BenchmarkStats {
    fn new() -> Self {
        Self {
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            total_duration: Duration::ZERO,
            latencies: Vec::new(),
            domain_counts: HashMap::new(),
        }
    }

    fn add_request(&mut self, latency: Option<Duration>, domain: String) {
        self.total_requests += 1;
        *self.domain_counts.entry(domain).or_insert(0) += 1;

        match latency {
            Some(lat) => {
                self.successful_requests += 1;
                self.latencies.push(lat);
            }
            None => {
                self.failed_requests += 1;
            }
        }
    }

    fn calculate_percentile(&self, percentile: f64) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }

        let mut sorted_latencies = self.latencies.clone();
        sorted_latencies.sort();

        let index = ((percentile / 100.0) * (sorted_latencies.len() - 1) as f64) as usize;
        sorted_latencies[index]
    }

    fn print_results(&self, args: &Args) {
        println!("\nAzolla create_task Benchmark Results");
        println!("====================================");
        println!("Server: {}", args.server);
        println!("Total Requests: {}", self.total_requests);
        println!("Concurrency: {} connections", args.concurrency);
        println!("Domains: {} (zipfian distribution)", args.domains);
        println!("Duration: {:.2}s", self.total_duration.as_secs_f64());
        println!();

        let rps = self.successful_requests as f64 / self.total_duration.as_secs_f64();
        let success_rate = (self.successful_requests as f64 / self.total_requests as f64) * 100.0;

        println!("Throughput: {rps:.0} requests/second");
        println!(
            "Success Rate: {:.2}% ({}/{})",
            success_rate, self.successful_requests, self.total_requests
        );
        println!();

        if !self.latencies.is_empty() {
            let avg_latency = self.latencies.iter().sum::<Duration>() / self.latencies.len() as u32;
            let min_latency = *self.latencies.iter().min().unwrap();
            let max_latency = *self.latencies.iter().max().unwrap();

            println!("Latency Distribution:");
            println!("  Average: {:.2}ms", avg_latency.as_secs_f64() * 1000.0);
            println!("  Minimum: {:.2}ms", min_latency.as_secs_f64() * 1000.0);
            println!("  Maximum: {:.2}ms", max_latency.as_secs_f64() * 1000.0);
            println!(
                "  P50: {:.2}ms",
                self.calculate_percentile(50.0).as_secs_f64() * 1000.0
            );
            println!(
                "  P95: {:.2}ms",
                self.calculate_percentile(95.0).as_secs_f64() * 1000.0
            );
            println!(
                "  P99: {:.2}ms",
                self.calculate_percentile(99.0).as_secs_f64() * 1000.0
            );
            println!();
        }

        println!("Domain Distribution:");
        // Sort domains by count (descending)
        let mut domain_vec: Vec<_> = self.domain_counts.iter().collect();
        domain_vec.sort_by(|a, b| b.1.cmp(a.1));

        for (domain, count) in domain_vec.iter().take(10) {
            let percentage = (**count as f64 / self.total_requests as f64) * 100.0;
            println!("  {domain}: {count} requests ({percentage:.1}%)");
        }

        if domain_vec.len() > 10 {
            println!("  ... ({} more domains)", domain_vec.len() - 10);
        }
    }
}

async fn create_grpc_client(server_addr: &str) -> Result<ClientServiceClient<Channel>> {
    let channel = tonic::transport::Channel::from_shared(server_addr.to_string())?
        .connect()
        .await?;

    Ok(ClientServiceClient::new(channel))
}

fn generate_random_task_data(
    task_prefix: &str,
    domain: &str,
    counter: usize,
    rng: &mut impl Rng,
) -> CreateTaskRequest {
    let task_name = format!(
        "{}_{}_{}_{}",
        task_prefix,
        domain,
        counter,
        rng.gen::<u16>()
    );

    // Generate random retry policy using internal representation
    let max_retries = rng.gen_range(1..=5) as u32;
    let mut retry_policy = InternalRetryPolicy::default();
    retry_policy.stop.max_attempts = Some(max_retries);

    if rng.gen_bool(0.5) {
        let delay = rng.gen_range(0.5..=2.0);
        retry_policy.wait.strategy = WaitStrategy::Fixed;
        retry_policy.wait.delay = Some(delay);
        retry_policy.wait.initial_delay = delay;
        retry_policy.wait.multiplier = 1.0;
        retry_policy.wait.max_delay = delay;
    } else {
        retry_policy.wait.strategy = WaitStrategy::ExponentialJitter;
        retry_policy.wait.initial_delay = rng.gen_range(0.5..=1.5);
        retry_policy.wait.multiplier = rng.gen_range(1.2..=2.5);
        retry_policy.wait.max_delay = rng.gen_range(5.0..=120.0);
        retry_policy.wait.delay = None;
    }

    retry_policy.retry.include_errors = vec!["ValueError".to_string(), "RuntimeError".to_string()];
    retry_policy.retry.exclude_errors.clear();
    let retry_policy_proto: Option<ProtoRetryPolicy> = Some(retry_policy.to_proto());

    // Generate random args (1-3 elements)
    let num_args = rng.gen_range(1..=3);
    let args: Vec<String> = (0..num_args).map(|i| format!("arg_{i}")).collect();

    // Generate random kwargs (1-3 key-value pairs)
    let num_kwargs = rng.gen_range(1..=3);
    let mut kwargs_obj = serde_json::Map::new();
    for i in 0..num_kwargs {
        kwargs_obj.insert(
            format!("key_{i}"),
            json!(format!("value_{}", rng.gen::<u16>())),
        );
    }

    CreateTaskRequest {
        name: task_name,
        domain: domain.to_string(),
        retry_policy: retry_policy_proto,
        args: serde_json::to_string(&args).unwrap_or_default(),
        kwargs: serde_json::to_string(&kwargs_obj).unwrap_or_default(),
        flow_instance_id: None,
        shepherd_group: None,
    }
}

fn generate_all_requests(args: &Args, zipf: &ZipfianDistribution) -> Vec<CreateTaskRequest> {
    let mut requests = Vec::with_capacity(args.requests);
    let mut rng = thread_rng();

    for i in 0..args.requests {
        let domain = zipf.select_domain(&mut rng);
        let request = generate_random_task_data(&args.task_prefix, domain, i, &mut rng);
        requests.push(request);
    }

    requests
}

async fn benchmark_worker(
    client: ClientServiceClient<Channel>,
    requests: Arc<Vec<CreateTaskRequest>>,
    start_idx: usize,
    end_idx: usize,
) -> Vec<(Option<Duration>, String)> {
    let request_count = end_idx - start_idx;
    let mut results = Vec::with_capacity(request_count);
    let mut client = client;

    for i in start_idx..end_idx {
        let request = &requests[i];
        let domain = request.domain.clone();
        let start = Instant::now();

        // Add timeout to prevent hanging requests
        let result = timeout(Duration::from_secs(30), client.create_task(request.clone())).await;

        let latency = match result {
            Ok(Ok(_)) => Some(start.elapsed()),
            Ok(Err(_)) | Err(_) => None,
        };

        results.push((latency, domain));
    }

    results
}

async fn run_benchmark(args: Args) -> Result<()> {
    println!("Connecting to server: {}", args.server);

    // Test connection first
    let test_client = create_grpc_client(&args.server).await?;
    drop(test_client);
    println!("Connection successful!");

    println!("\nGenerating {} requests...", args.requests);
    let zipf = ZipfianDistribution::new(args.domains);

    // Pre-generate all requests
    let all_requests = Arc::new(generate_all_requests(&args, &zipf));

    println!("Starting benchmark...");
    println!("Requests: {}", args.requests);
    println!("Concurrency: {}", args.concurrency);
    println!("Domains: {}", args.domains);

    let requests_per_worker = args.requests / args.concurrency;
    let remaining_requests = args.requests % args.concurrency;

    // Calculate chunk boundaries
    let mut chunk_boundaries = Vec::new();
    let mut start_idx = 0;

    for worker_id in 0..args.concurrency {
        let worker_request_count = if worker_id < remaining_requests {
            requests_per_worker + 1
        } else {
            requests_per_worker
        };

        let end_idx = start_idx + worker_request_count;
        chunk_boundaries.push((start_idx, end_idx));
        start_idx = end_idx;
    }

    let start_time = Instant::now();

    // Create concurrent workers
    let mut handles = Vec::new();

    for (start_idx, end_idx) in chunk_boundaries.into_iter() {
        let client = create_grpc_client(&args.server).await?;
        let requests_ref = Arc::clone(&all_requests);

        let handle = tokio::spawn(async move {
            benchmark_worker(client, requests_ref, start_idx, end_idx).await
        });

        handles.push(handle);
    }

    // Wait for all workers to complete
    let mut stats = BenchmarkStats::new();
    for handle in handles {
        let worker_results = handle.await?;
        for (latency, domain) in worker_results {
            stats.add_request(latency, domain);
        }
    }

    stats.total_duration = start_time.elapsed();
    stats.print_results(&args);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if let Err(e) = run_benchmark(args).await {
        eprintln!("Benchmark failed: {e}");
        std::process::exit(1);
    }

    Ok(())
}
