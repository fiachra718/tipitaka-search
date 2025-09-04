# 1) start clean
curl -u elastic:changeme -XDELETE "http://localhost:9200/bilara_segments"

# 2) load Pāli root + Sujato EN for DN/MN/SN/AN in one shot
python3 bilara_loader_dropin.py \
  "/home/andrew/sc-data/sc_bilara_data/root/pli/ms/sutta/dn/**/*.json" \
  "/home/andrew/sc-data/sc_bilara_data/root/pli/ms/sutta/mn/**/*.json" \
  "/home/andrew/sc-data/sc_bilara_data/root/pli/ms/sutta/sn/**/*.json" \
  "/home/andrew/sc-data/sc_bilara_data/root/pli/ms/sutta/an/**/*.json" \
  "/home/andrew/sc-data/sc_bilara_data/translation/en/sujato/sutta/dn/**/*.json" \
  "/home/andrew/sc-data/sc_bilara_data/translation/en/sujato/sutta/mn/**/*.json" \
  "/home/andrew/sc-data/sc_bilara_data/translation/en/sujato/sutta/sn/**/*.json" \
  "/home/andrew/sc-data/sc_bilara_data/translation/en/sujato/sutta/an/**/*.json" \
  --index bilara_segments --refresh 2>reindex.log

tail -100 reindex.log
