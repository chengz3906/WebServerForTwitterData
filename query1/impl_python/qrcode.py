from flask import Flask, request, jsonify

app = Flask(__name__)

def encode(d):
    return 'Encoded'

def decode(d):
    return 'Decoded'

@app.route("/healthcheck")
def handleHeartbeat():
    return "Good\n"

@app.route("/q1", methods=['GET'])
def handle():
    reqType = request.args['type']
    reqData = request.args['data']
    if reqType == 'encode':
        res = encode(reqData)
    elif reqType == 'decode':
        res = decode(reqData)
    else:
        return "Wrong type!\n"
    return res + '\n'

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000, processes = 4, threaded = False)
