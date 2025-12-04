# Production Deployment: Comprehensive Analysis & Recommendation

## Current Status
- **Audit Grade**: A- (Ready for production)
- **Critical Fixes**: Completed (run_etl.sh created, imports optimized)
- **Existing Infrastructure**: User already has `.github/workflows/etl.yml`

## Deployment Options Analysis

### Option 1: GitHub Actions (Scheduled) ⭐ RECOMMENDED
**Best for**: Daily/hourly batch ELT jobs with predictable schedules

**Pros:**
- ✅ Already have infrastructure (`.github/workflows/etl.yml`)
- ✅ Free for public repos, 2000 min/month for private
- ✅ Direct GitHub integration (version control + execution)
- ✅ Simple secret management (`POSTGRES_URI`, etc.)
- ✅ No server management required
- ✅ Easy debugging (logs in GitHub UI)

**Cons:**
- ❌ Can't run more frequently than every 5 minutes
- ❌ Maximum 6 hours per job
- ❌ Potential cold starts (minimal for batch jobs)

**Cost**: $0 - $4/month (likely free within limits)

**Implementation**:
```yaml
# .github/workflows/etl.yml (already exists!)
on:
  schedule:
    - cron: '0 3 * * *'  # Daily at 3 AM UTC
  workflow_dispatch: {}   # Manual trigger
```

**Action Required:**
1. Fix entry point: Current workflow calls `src.main`, but we use `scripts/run_elt.py`
2. Add all secrets to GitHub repo settings
3. Test with `workflow_dispatch`

---

### Option 2: Supabase Edge Functions
**Best for**: Event-driven, real-time processing

**Pros:**
- ✅ Native Supabase integration
- ✅ Serverless (auto-scaling)
- ✅ Database in same network (low latency)

**Cons:**
- ❌ Deno runtime (TypeScript/JavaScript) - would need complete rewrite
- ❌ 256MB memory limit (might not fit our dependencies)
- ❌ Limited to 10 GB-hours/month in free tier
- ❌ Not designed for long-running batch jobs

**Cost**: $0 - $25/month

**Verdict**: ❌ Not suitable for Python-based batch ELT

---

### Option 3: Google Cloud Run (Serverless Containers)
**Best for**: Flexible, containerized workloads with unpredictable load

**Pros:**
- ✅ Runs Docker containers (our Dockerfile works as-is)
- ✅ Auto-scales to zero (cost-effective)
- ✅ Can be triggered by Cloud Scheduler
- ✅ 2M requests/month free tier

**Cons:**
- ❌ Requires Google Cloud account setup
- ❌ More complex than GitHub Actions
- ❌ Cold start delays (5-10 seconds)

**Cost**: $5 - $15/month (with free tier)

**Implementation**:
```bash
# Deploy
gcloud run deploy elt-pipeline --source .
# Schedule
gcloud scheduler jobs create http etl-daily \
  --schedule="0 3 * * *" \
  --uri=https://elt-pipeline-xxx.run.app
```

---

### Option 4: AWS Lambda + EventBridge
**Best for**: AWS-centric infrastructure

**Pros:**
- ✅ Full AWS ecosystem integration
- ✅ 1M free requests/month
- ✅ EventBridge for scheduling

**Cons:**
- ❌ 10 GB package size limit (might be tight with pandas)
- ❌ 15-minute maximum runtime (borderline for 20k records)
- ❌ Requires Lambda container image or layers
- ❌ Steeper learning curve

**Cost**: $5 - $20/month

---

## Final Recommendation

### **Use GitHub Actions (Option 1)**

**Rationale:**
1. **Already implemented**: User has `.github/workflows/etl.yml`
2. **Cost-effective**: Likely free within 2000 min/month
3. **Simple**: No infrastructure to manage
4. **Sufficient**: Daily batch jobs don't need real-time processing
5. **Transparent**: Logs and history in GitHub UI

### **As a Backup: Cloud Run (Option 3)**

If GitHub Actions hits limits:
- Deploy Docker image to Cloud Run
- Trigger via Cloud Scheduler
- Minimal code changes required

---

## Implementation Steps for GitHub Actions

### 1. Fix Entry Point Issue
**Problem**: Current `.github/workflows/etl.yml` calls `python -m src.main`, but entry point is `scripts/run_elt.py`

**Solution**: Update workflow to use correct entry point

### 2. Add Secrets
In GitHub repo → Settings → Secrets and variables → Actions:
- `POSTGRES_URI`
- `SUPABASE_SERVICE_KEY`
- `SUPABASE_URL`
- `SHEETS_SA_JSON`
- (Other API keys as needed)

### 3. Test
- Go to Actions tab
- Select "ETL Daily" workflow
- Click "Run workflow" (manual trigger)
- Monitor logs

### 4. Monitor
- Check workflow runs daily
- Review logs for errors
- Set up email notifications for failures (GitHub → repo settings → Notifications)

---

## Migration Path (if needed later)

If requirements change:
1. **More frequent runs** → Add more cron schedules or switch to Cloud Run + Scheduler
2. **Longer runtime** → Cloud Run (no time limit)
3. **Real-time processing** → Implement webhook → Cloud Run/Lambda
