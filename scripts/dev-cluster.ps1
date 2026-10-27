# 5-node local cluster on Windows: 3 raft voters + 2 storage-only, k=3 m=2. Ctrl-C stops everything.
$root = Split-Path $PSScriptRoot
Set-Location $root
go build -o bin ./cmd/...
$voters = "n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003"
$procs = 1..5 | ForEach-Object {
    Start-Process -PassThru -NoNewWindow "$root\bin\objstored.exe" -ArgumentList @(
        "--id", "n$_", "--listen", "127.0.0.1:900$_", "--data-dir", "$root\.dev-data\n$_", "--zone", "rack$_",
        "--voters", $voters, "-k", "3", "-m", "2", "--shard-size", "65536", "--repair-delay", "20s",
        "--s3-listen", "127.0.0.1:910$_", "--s3-access-key", "devkey", "--s3-secret-key", "devsecret123")
}
Write-Host "cluster up: bin\objctl.exe -e http://127.0.0.1:9001 status  (S3: 127.0.0.1:9101, devkey/devsecret123)"
try { $procs | Wait-Process } finally { $procs | Stop-Process -Force -ErrorAction SilentlyContinue }
