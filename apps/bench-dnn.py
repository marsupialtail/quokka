from transformers import pipeline
import time
generator = pipeline('text-generation', model="facebook/opt-125m")
print(time.time())
print(generator("Are you at home"))
print(time.time())
