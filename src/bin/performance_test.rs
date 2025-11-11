use HMSClipTest_rs::performance_test::main_performance_test;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    main_performance_test().await
}
