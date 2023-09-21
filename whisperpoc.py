import whisper, json, os, time
from azureutils import BlobUtil

model = whisper.load_model("medium")

bloblist = ['08A3C1EB11BD4F96B16829A326E911D2.wav',
            '3F49355EEB0141619303CD4500DFD62D.wav',
            'BB0562731F924FD0AF9533EBB1E31DA9.wav',
            'C457CB63CFF14BA68AFF7D9DB0905995.wav']

bu = BlobUtil(os.getenv('BLOB_CONN'),'whisper')

t0 = time.time()

for blobname in bloblist:
    bu.download_binary_blob(blobname, 'transcribeme.wav')

    ffmpegcmd = "ffmpeg -y -i transcribeme.wav -map_channel 0.0.0 left.wav -map_channel 0.0.1 right.wav"
    os.system(ffmpegcmd)

    result = model.transcribe("left.wav")
    lfile = blobname.replace('.wav','-service.json')
    with open('lefttranscript.json', "w") as f:
        json.dump(result["segments"], f, indent = 4)

    bu.upload_binary_blob(f'transcribe/{lfile}','lefttranscript.json')

    result = model.transcribe("right.wav")
    rfile = blobname.replace('.wav','-caller.json')
    with open('righttranscript.json', "w") as f:
        json.dump(result["segments"], f, indent = 4)

    bu.upload_binary_blob(f'transcribe/{rfile}','righttranscript.json')

t1 = time.time()

total = t1-t0

print(f'total time in seconds: {total}')

