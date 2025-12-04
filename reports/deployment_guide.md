# GitHub Actions Deployment Guide

## Prerequisites
- ✅ Code audit completed (Grade A-)
- ✅ All tests passing (27/27)
- ✅ Workflow file created (`.github/workflows/etl.yml`)
- ✅ Entry point fixed (`scripts/run_elt.py`)

---

## Step 1: Add Secrets to GitHub

1. Go to your GitHub repository
2. Navigate to: **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret** for each:

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `POSTGRES_URI` | Supabase/PostgreSQL connection string | `postgresql://user:pass@host:5432/db` |
| `SUPABASE_URL` | Your Supabase project URL | `https://xxx.supabase.co` |
| `SUPABASE_SERVICE_KEY` | Service role key (not anon key!) | `eyJhbGc...` |
| `SHEETS_SA_JSON` | Google Sheets service account JSON | `{"type":"service_account",...}` |
| `BITRIX_WEBHOOK` | (Optional) Bitrix24 webhook URL | |
| `GOOGLE_ADS_TOKEN` | (Optional) Google Ads API token | |
| `YANDEX_DIRECT_TOKEN` | (Optional) Yandex Direct token | |
| `META_TOKEN` | (Optional) Meta/Facebook token | |
| `YOUTUBE_KEY` | (Optional) YouTube API key | |

**Critical**: Use **Service Role Key** for Supabase, not the anon key!

---

## Step 2: Test Workflow Manually

1. Go to: **Actions** tab in your GitHub repo
2. Select **ETL Daily** from the workflows list
3. Click **Run workflow** (on the right side)
4. Select branch (usually `main` or `master`)
5. Click green **Run workflow** button

**Expected behavior**:
- Workflow starts immediately
- Steps execute: checkout → setup Python → install deps → migrations → ETL
- Status shows green checkmark ✅

---

## Step 3: Monitor Execution

### View Logs
1. Click on the running/completed workflow run
2. Click on **run-etl** job
3. Expand each step to see detailed logs

### Check for Errors
Common issues:
- **Secret not found**: Double-check secret names match exactly
- **Database connection failed**: Verify `POSTGRES_URI` format
- **Import errors**: Check if all dependencies in `requirements.txt`

---

## Step 4: Verify Schedule

The workflow runs **daily at 3 AM UTC** (see `.github/workflows/etl.yml`):
```yaml
on:
  schedule:
    - cron: '0 3 * * *'
```

To change schedule:
- `'0 */6 * * *'` = Every 6 hours
- `'0 1 * * *'` = Daily at 1 AM UTC
- `'0 0 * * 0'` = Weekly (Sundays at midnight)

---

## Step 5: Setup Notifications (Optional)

### Email Notifications
GitHub sends emails for failed workflows automatically if you have notifications enabled.

**To configure**:
1. GitHub → Settings (your profile, not repo)
2. Notifications → Actions
3. Enable "Send notifications for failed workflows"

### Slack/Telegram (Future)
Add webhook integration:
```yaml
- name: Notify on failure
  if: failure()
  run: |
    curl -X POST ${{ secrets.SLACK_WEBHOOK }} \
      -d '{"text":"ETL failed! Check logs."}'
```

---

## Monitoring & Maintenance

### Daily Checks
- ✅ Workflow completes successfully
- ✅ Data appears in `staging.records`
- ✅ No error logs in GitHub Actions

### Weekly Checks
- Review error rates in logs
- Check data quality metrics
- Verify incremental loading (0 records if no changes)

### Troubleshooting

**Issue**: Workflow doesn't trigger on schedule
- **Solution**: Workflows won't auto-run in inactive repos. Make a commit to activate.

**Issue**: Out of minutes (2000/month limit)
- **Solution**: Optimize runtime or upgrade to GitHub Pro ($4/month for unlimited private repo minutes)

**Issue**: Job timeout (6 hour limit)
- **Solution**: Unlikely for 20k records. If hit, consider Cloud Run.

---

## Cost Estimate

- **Free tier**: 2000 minutes/month
- **Current usage**: ~5 min/day = 150 min/month
- **Cost**: **$0/month** ✅

If you exceed:
- **GitHub Pro**: $4/month (unlimited private repo minutes)
- **Alternative**: Cloud Run (~$5-10/month)

---

## Next Steps After Deployment

1. Monitor for 1 week to ensure stability
2. Add data quality tests in workflow
3. Implement Slack alerting for failures
4. Consider adding data validation step
