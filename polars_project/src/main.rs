use polars::prelude::*;
use anyhow::{Result, anyhow};
use std::env;
use std::time::Instant;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        return Err(anyhow!("Please provide a query string, e.g., age>25,country=US"));
    }

    let query_str = &args[1];
    let iterations = 10;
    let mut total_duration = 0.0;

    for i in 0..iterations {
        let df = LazyCsvReader::new("data/sample.csv")
            .has_header(true)
            .finish()?;

        let mut filtered_df = df;

        for condition in query_str.split(',') {
            let condition = condition.trim();
            if condition.contains('>') {
                let parts: Vec<&str> = condition.split('>').collect();
                let col_name = parts[0].trim();
                let val: i64 = parts[1].trim().parse()?;
                filtered_df = filtered_df.filter(polars::prelude::col(col_name).gt(lit(val)));
            } else if condition.contains('<') {
                let parts: Vec<&str> = condition.split('<').collect();
                let col_name = parts[0].trim();
                let val: i64 = parts[1].trim().parse()?;
                filtered_df = filtered_df.filter(polars::prelude::col(col_name).lt(lit(val)));
            } else if condition.contains('=') {
                let parts: Vec<&str> = condition.split('=').collect();
                let col_name = parts[0].trim();
                let val = parts[1].trim().replace("'", "").replace("\"", "");
                filtered_df = filtered_df.filter(polars::prelude::col(col_name).eq(lit(val)));
            }
        }

        let start = Instant::now();
        let result = filtered_df.collect()?;
        let duration = start.elapsed().as_secs_f64();
        total_duration += duration;

        if i == 0 {
            println!("--- Sample Result (Run #1) ---");
            println!("{}", result);
        }

        println!("Run {:>2}: {:.6} seconds", i + 1, duration);
    }

    let avg_time = total_duration / iterations as f64;
    println!("\n✅ Completed {} runs. Average execution time: {:.6} seconds", iterations, avg_time);

    Ok(())
}
