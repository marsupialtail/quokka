import pyquokka
qc = pyquokka.QuokkaContext()

def analyze_youtube_parquet():
    youtube_videos = qc.read_parquet("/home/ziheng/Downloads/youtube.parquet")
    