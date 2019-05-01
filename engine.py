import os
from pyspark.mllib.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.ml.feature import IndexToString, StringIndexer
from pyspark.sql import *
import pandas

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_counts_and_averages(ID_and_ratings_tuple):
    """Given a tuple (movieID, ratings_iterable) 
    returns (movieID, (ratings_count, ratings_avg))
    """
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)


class RecommendationEngine:
    """A movie recommendation engine
    """

    def __count_and_average_ratings(self):
        """Updates the movies ratings counts from 
        the current data self.ratings_RDD
        """
        logger.info("Counting movie ratings...")
        self.rating_count= self.datas.count()


    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        als = ALS(maxIter=5, regParam=0.01, userCol="uid", itemCol="iid", ratingCol="rating",
                coldStartStrategy="drop")
        self.model = als.fit(self.to_be_train)
        logger.info("ALS model built!")

    def __predict_ratings(self, x_df):
        """Gets predictions for a given (userID, movieID) formatted RDD
        Returns: an RDD with format (movieTitle, movieRating, numRatings)
        """
        predicted = self.model.transform(x_df)
        return predicted
    
    def get_rec_user(self, count):
        """Add additional movie ratings in the format (user_id, movie_id, rating)
        """
        ratings=self.model.recommendForAllItems(count)
        
        return ratings.toJSON().collect()

    def get_ratings_for_movie_ids(self, user_id, movie_ids):
        """Given a user_id and a list of movie_ids, predict ratings for them 
        """
        requested_movies_RDD = self.sc.parallelize(movie_ids).map(lambda x: (user_id, x))
        # Get predicted ratings
        ratings = self.__predict_ratings(requested_movies_RDD.toDF(['uid','iid'])).toJSON().collect()
        return ratings
    
    def get_top_ratings(self, movies_count):
        """Recommends up to movies_count top unrated movies to user_id
        """
        logger.info("Generating rec kindle for all user")
        ratings = self.model.recommendForAllUsers(movies_count)

        return ratings.toJSON().collect()

    def __init__(self, sc, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        logger.info("Starting up the Recommendation Engine: ")

        self.sc = sc

        # Load movies data for later use
        logger.info("Loading Movies data...")

        data_file_path = os.path.join(dataset_path, 'kindle-store.csv')
        df = sc.textFile(data_file_path)
        df=df.map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1],float(tokens[2]),tokens[3])).toDF(['user_id','item_id','rating','timestamps'])

        stringindexer = StringIndexer(inputCol='user_id',outputCol='user_id_int')
        stringindexer.setHandleInvalid("keep")
        model = stringindexer.fit(df)
        indexed = model.transform(df)
        self.uid_indexer = model

        stringindexer_item = StringIndexer(inputCol='item_id',outputCol='item_id_int')
        stringindexer_item.setHandleInvalid("keep") 
        model = stringindexer_item.fit(indexed)
        indexed = model.transform(indexed)
        self.iid_indexer = model

        self.datas=df
        self.to_be_train=indexed.selectExpr(['user_id_int as uid','item_id_int as iid','rating as rating'])
        # Pre-calculate movies ratings counts
        self.__count_and_average_ratings()

        # Train the model
        self.rank = 8
        self.seed = 5
        self.iterations = 10
        self.regularization_parameter = 0.1
        self.__train_model() 
