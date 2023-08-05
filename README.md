# Gen3 Ingester App

```
ssh nantucket
source /proj/relibs/relib00/conda-cdnm/bin/activate
conda activate /udd/rejpz/.conda/envs/dash-2.0.0

git clone https://changit.bwh.harvard.edu/gen3/gen3-ingester-app
cd gen3-ingester-app

python DashApp/dash-app.py --configfile conf/chandemo5.yaml
```

Then visit (requires VPN): http://nantucket:8050/

