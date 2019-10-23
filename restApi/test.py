from flask import Flask , render_template,request, Response
from downloadData import *
import json

app = Flask(__name__)

@app.route('/')
def index() :
    return 'Server Works'

@app.route('/hi')
def hi() :
    return render_template('index.html')

@app.route('/v1/traffic',methods=['GET','POST'])
def traffic() :
    if request.method == 'GET' :
        nodeid = request.args.get('nodeId')
        # print('node id is {0}'.format(nodeid))
        data = restAPIData()
        # print(data)
        # return 'this is a api service, you have to use post method <br> check documentation'
        return Response(json.dumps(data),  mimetype='application/json')
    if request.method == 'POST' :
        nodeid = request.args.get('nodeId')
        return 'this is a post method'+ nodeid


if __name__ == '__main__' :
    app.run()