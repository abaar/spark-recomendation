from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import RecommendationEngine
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request
 
@main.route("/user-to-kindle/<int:count>", methods=["GET"])
def top_ratings(count):
    logger.debug("User %s TOP ratings requested", count)
    top_ratings = recommendation_engine.get_top_ratings(count)
    return json.dumps(top_ratings)
 
@main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
def movie_ratings(user_id, movie_id):
    logger.debug("User %s rating requested for movie %s", user_id, movie_id)
    ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
    return json.dumps(ratings)
 
 
@main.route("/kindle-to-user/<int:count>/", methods = ["GET"])
def get_rec_user(count):
    logger.debug("User %s TOP ratings requested", count)
    recommendation_engine.get_rec_user(ratings)
    return json.dumps(ratings)
 
 
def create_app(spark_context, dataset_path):
    global recommendation_engine 

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app 
