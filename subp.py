import sys, whisper, json

model = whisper.load_model("medium")

result = model.transcribe(sys.argv[1])

with open(sys.argv[2], "w") as f:
    json.dump(result["segments"], f, indent = 4)
    