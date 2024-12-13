import mysql.connector
import random
import simplejson
from datetime import datetime
import time
from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaError




# MySQL connection details
host = 'localhost'
user = 'root'
password = 'admin.'
db = 'voting_sys'

config = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(config)

if __name__ == '__main__':
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=db
        )
        curr = conn.cursor()

        # Fetch candidates
        curr.execute("""
            SELECT JSON_ARRAYAGG(
                JSON_OBJECT(
                    'candidate_id', candidate_id,
                    'candidate_name', candidate_name,
                    'party_affiliation', party_affiliation,
                    'biography', biography,
                    'campaign_platform', campaign_platform,
                    'photo_url', photo_url
                )
            ) AS json_result
            FROM candidates;
        """)
        result = curr.fetchone()
        candidates = simplejson.loads(result[0]) if result[0] else []

        if not candidates:
            raise Exception('No candidates found')

        # Fetch all voters
        curr.execute("SELECT voter_id FROM voters;")
        voters = [row[0] for row in curr.fetchall()]

        if not voters:
            raise Exception('No voters found')

        # Create votes for each voter
        for voter_id in voters:
            chosen_candidate = random.choice(candidates)

            # Combine voter and candidate data
            vote = {
                'voter_id': voter_id,
                **chosen_candidate,
                'voting_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'vote': 1
            }
            try:
                print(f'user {vote["voter_id"]} is voting for candidate {vote["candidate_id"]}')

                # Insert vote into MySQL
                curr.execute("""
                    INSERT INTO votes(voter_id, candidate_id, voting_time)
                    VALUES (%s, %s, %s)
                """, (vote["voter_id"], vote["candidate_id"], vote["voting_time"]))
                conn.commit()

                # Produce message to Kafka
                producer.produce(
                    topic='votes_topic',
                    key=str(vote['voter_id']),
                    value=simplejson.dumps(vote),
                )
                producer.poll(0)
                print('Vote produced to Kafka ')

            except Exception as e:
                print(f'Error inserting value into database: {e}')
        
                continue
           

    except Exception as e:
        print(f'Error in voting system: {e}')

    finally:
     
        producer.flush()
