from flask import Flask, request, make_response, jsonify
from flask_restful import Resource, Api
import os

from flask.ext.httpauth import HTTPBasicAuth
from common.dateRoutines import Dates

from app.resources import InvalidUsage
from app.resources import leastfares
from app.resources import bookings
from app.resources import orgs
from app.resources import counts

app = Flask(__name__,static_url_path="")
api = Api(app)

dateObject = Dates()

# auth = HTTPBasicAuth()
# @auth.get_password
# def get_password(username):
#     if username == 'srini':
#         return 'ins01'
#     return None
# @auth.error_handler
# def unauthorized():
#     # return 403 instead of 401 to prevent browsers from displaying the default
#     # auth dialog
#     return make_response(jsonify({'message': 'Unauthorized access'}), 403)

@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


api.add_resource(leastfares, '/api/v1.0/leastfares/<essource>/<cp>')
api.add_resource(bookings, '/api/v1.0/bookings/<essource>/<clientID>')
api.add_resource(orgs, '/api/v1.0/clients/<essource>')
api.add_resource(counts, '/api/v1.0/counts/<essource>')
#@app.route('/api/v1.0/bookings/<ds>/<clientID>')

if __name__ == '__main__':
    #app.run(debug=True, port=8000)
    port = int(os.environ.get("PORT", 5000))
    app.run(host='instapi.herokuapp.com', port=port)