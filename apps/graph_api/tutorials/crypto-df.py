from pyquokka.df import QuokkaContext
import os

qc = QuokkaContext()

items = qc.read_rest_get("https://hacker-news.firebaseio.com/v0/item/", ["{}.json".format(i) for i in range(30000000,30020000)], ["by","type","id","title","url","score","time"])
items = items.filter("type = 'story' and url REGEXP '^https://github.com/[^/]+/[^/]+$'")
items.explain()
results = items.collect()
print(results)

gh_key = os.environ["GITHUB_KEY"]
urls = [url.replace("https://github.com/", "") for url in results["url"].to_list()]
print(urls)

repos = qc.read_rest_get("https://api.github.com/repos/", urls , ["updated_at"] , headers =  {"Authorization": "token " + gh_key}, batch_size=100)
print(repos.collect())