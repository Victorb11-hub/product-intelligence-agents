import os, sys, time

# Manual .env parse
env_path = os.path.join(os.path.dirname(__file__), "agents", ".env")
with open(env_path, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "agents"))
from config import get_supabase

sb = get_supabase()
PRODUCT_ID = "f0620e1e-83fd-45c9-ac92-dd922e4c674c"

def paginated_count(table_name, filters=None):
    """Count all rows matching filters via pagination (1000-row pages), selecting only id."""
    total = 0
    offset = 0
    PAGE = 1000
    while True:
        q = sb.table(table_name).select("id").limit(PAGE).offset(offset)
        if filters:
            for col, op, val in filters:
                if op == "eq":
                    q = q.eq(col, val)
                elif op == "is_":
                    q = q.is_(col, val)
                elif op == "not.in":
                    q = q.not_.in_(col, val)
                elif op == "neq":
                    q = q.neq(col, val)
        resp = q.execute()
        n = len(resp.data)
        total += n
        if n < PAGE:
            break
        offset += PAGE
    return total

def paginated_group_count(table_name, group_col, filters=None, extra_group=None):
    """Group count via pagination. Returns dict of {group_val: count} or {(g1,g2): count}."""
    results = {}
    offset = 0
    PAGE = 1000
    cols = group_col if not extra_group else f"{group_col},{extra_group}"
    while True:
        q = sb.table(table_name).select(cols).limit(PAGE).offset(offset)
        if filters:
            for col, op, val in filters:
                if op == "eq":
                    q = q.eq(col, val)
                elif op == "is_":
                    q = q.is_(col, val)
                elif op == "not.in":
                    q = q.not_.in_(col, val)
        resp = q.execute()
        n = len(resp.data)
        for row in resp.data:
            if extra_group:
                key = (row.get(group_col, "NULL"), row.get(extra_group, "NULL"))
            else:
                key = row.get(group_col, "NULL")
            results[key] = results.get(key, 0) + 1
        if n < PAGE:
            break
        offset += PAGE
    return results

print("=" * 60)
print("COMMENT COUNT RECONCILIATION")
print(f"Product: {PRODUCT_ID}")
print("=" * 60)
print()

# TABLE 1: comments table (product-filtered)
print("TABLE 1: comments table (product-filtered)")
print("-" * 40)
t1 = paginated_group_count("comments", "platform", [("product_id", "eq", PRODUCT_ID)])
total1 = 0
print(f"{'Platform':<20} | {'Count':>8}")
print(f"{'-'*20}-+-{'-'*8}")
for p in sorted(t1):
    print(f"{str(p):<20} | {t1[p]:>8}")
    total1 += t1[p]
print(f"{'TOTAL':<20} | {total1:>8}")
print()

# TABLE 2: posts table data_type='comment' (product-filtered)
print("TABLE 2: posts table data_type='comment' (product-filtered)")
print("-" * 40)
t2 = paginated_group_count("posts", "platform", [("product_id", "eq", PRODUCT_ID), ("data_type", "eq", "comment")])
total2 = 0
print(f"{'Platform':<20} | {'Count':>8}")
print(f"{'-'*20}-+-{'-'*8}")
for p in sorted(t2):
    print(f"{str(p):<20} | {t2[p]:>8}")
    total2 += t2[p]
print(f"{'TOTAL':<20} | {total2:>8}")
print()

# TABLE 3: posts table data_type='post' (product-filtered)
print("TABLE 3: posts table data_type='post' (product-filtered)")
print("-" * 40)
t3 = paginated_group_count("posts", "platform", [("product_id", "eq", PRODUCT_ID), ("data_type", "eq", "post")])
total3 = 0
print(f"{'Platform':<20} | {'Count':>8}")
print(f"{'-'*20}-+-{'-'*8}")
for p in sorted(t3):
    print(f"{str(p):<20} | {t3[p]:>8}")
    total3 += t3[p]
print(f"{'TOTAL':<20} | {total3:>8}")
print()

# TABLE 4: posts table uncategorized (product-filtered)
print("TABLE 4: posts table uncategorized (product-filtered)")
print("-" * 40)
# Get all posts for this product, then filter client-side for NULL/other data_type
uncategorized = {}
offset = 0
PAGE = 1000
total4 = 0
while True:
    q = sb.table("posts").select("platform,data_type").eq("product_id", PRODUCT_ID).limit(PAGE).offset(offset)
    resp = q.execute()
    n = len(resp.data)
    for row in resp.data:
        dt = row.get("data_type")
        if dt not in ("post", "comment"):
            plat = row.get("platform", "NULL")
            dt_str = str(dt) if dt is not None else "NULL"
            key = (plat, dt_str)
            uncategorized[key] = uncategorized.get(key, 0) + 1
    if n < PAGE:
        break
    offset += PAGE

print(f"{'Platform':<20} | {'data_type':<15} | {'Count':>8}")
print(f"{'-'*20}-+-{'-'*15}-+-{'-'*8}")
for (p, dt) in sorted(uncategorized):
    print(f"{p:<20} | {dt:<15} | {uncategorized[(p,dt)]:>8}")
    total4 += uncategorized[(p, dt)]
print(f"{'TOTAL':<20} | {'':<15} | {total4:>8}")
print()

# TABLE 5: System-wide totals
print("TABLE 5: System-wide totals")
print("-" * 40)
sw_comments = paginated_count("comments")
sw_posts = paginated_count("posts")
sw_posts_post = paginated_count("posts", [("data_type", "eq", "post")])
sw_posts_comment = paginated_count("posts", [("data_type", "eq", "comment")])
sw_other = sw_posts - sw_posts_post - sw_posts_comment
print(f"comments table total:     {sw_comments}")
print(f"posts table total:        {sw_posts}")
print(f"  data_type='post':       {sw_posts_post}")
print(f"  data_type='comment':    {sw_posts_comment}")
print(f"  other/null:             {sw_other}")
print()

# TABLE 6: RECONCILIATION
print("TABLE 6: RECONCILIATION")
print("-" * 40)
diff = total1 - total2
if diff > 0:
    bigger = "comments table has more"
elif diff < 0:
    bigger = "posts table has more"
else:
    bigger = "exact match"
print(f"comments table (product):            {total1}")
print(f"posts data_type='comment' (product): {total2}")
print(f"Difference:                          {abs(diff)} ({bigger})")
print()
print(f"Grand total comment records:         {total1} (comments table total for product)")
print(f"Grand total all records:             {sw_posts + sw_comments} (posts total + comments total, system-wide)")
print()
print("Done.")
