import mysql.connector
import requests 
import random
import simplejson
import confluent_kafka as kafka
from datetime import datetime

from datetime import datetime


host='127.0.0.1'
user='root'
password='admin.'
port = '3306'
db = 'voting_sys'
BASE_URL = "https://randomuser.me/api/"
PARTIES = [ 'Progressive Future Alliance (PFA)' , 'United Peoples Front (UPF)' , 'Green Prosperity Movement (GPM)' , 'Liberty and Justice Party (LJP)' , 'National Renewal Coalition (NRC)' ]

random.seed(25)


def create_table(conn, cur):
        cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(36) PRIMARY KEY, -- UUID length
            candidate_name VARCHAR(100), -- 100 is sufficient for names
            party_affiliation VARCHAR(50), -- 50 is standard for political party names
            biography TEXT, -- No length restriction for detailed text
            campaign_platform TEXT, -- No length restriction for detailed text
            photo_url VARCHAR(2083) -- Max length for URLs as per RFC 3986
        );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS voters (
                voter_id VARCHAR(36) PRIMARY KEY, -- UUID length
                voter_name VARCHAR(100), -- 100 is sufficient for names
                date_of_birth DATE, -- Use DATE type for dates
                gender ENUM('Male', 'Female', 'Other'), -- Standard gender options
                nationality VARCHAR(50), -- 50 is sufficient for nationalities
                registration_number VARCHAR(50), -- Registration numbers are typically short
                address_street VARCHAR(150), -- 150 for detailed street addresses
                address_city VARCHAR(100), -- 100 for city names
                address_state VARCHAR(100), -- 100 for state/province names
                address_country VARCHAR(100), -- 100 for country names
                address_postcode VARCHAR(20), -- 20 for postal/ZIP codes
                email VARCHAR(254), -- Standard max length for emails as per RFC 5321
                phone_number VARCHAR(20), -- International phone number format
                cell_number VARCHAR(20), -- International phone number format
                picture TEXT, -- No length restriction for image paths/URLs
                registered_age TINYINT UNSIGNED -- Age is typically a small non-negative number
            ); 
            """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(36), -- UUID length
            candidate_id VARCHAR(36), -- UUID length
            voting_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Default to current time
            vote TINYINT DEFAULT 1, -- Use TINYINT for boolean-like fields
            PRIMARY KEY (voter_id, candidate_id),
            FOREIGN KEY (voter_id) REFERENCES voters(voter_id),
            FOREIGN KEY (candidate_id) REFERENCES candidates(candidate_id)
        );
    """)

def generate_candidate_data(can_num, tot_parties):
    res = requests.get(BASE_URL)
    if res.status_code == 200:
        user_data = res.json()['results'][0]
        return {
            'candidate_id': user_data['login']['uuid'],
            'candidate_name': f"{user_data['name']['first']} {user_data['name']['last']}",
            'party_affiliation': PARTIES[can_num % tot_parties],
            'biography' : '-------',
            'campaign_platform' : '-------',
            'photo_url' : user_data['picture']['large']
            
        }
    



def gen_voter_data():
    res = requests.get(BASE_URL)
    if res.status_code == 200:
        # user_data = res.json()['results'][0]
        user = res.json()["results"][0]
        return {
            "voter_id": user["login"]["uuid"],
            "voter_name": f"{user['name']['first']} {user['name']['last']}",
            "date_of_birth": user["dob"]["date"][:10], 
            "gender": user["gender"],
            "nationality": user["nat"],
            "registration_number": user["login"]["username"],
            "address_street": f"{user['location']['street']['number']} {user['location']['street']['name']}",
            "address_city": user["location"]["city"],
            "address_state": user["location"]["state"],
            "address_country": user["location"]["country"],
            "address_postcode": str(user["location"]["postcode"]),
            "email": user["email"],
            "phone_number": user["phone"],
            "cell_number": user["cell"],
            "picture": user["picture"]["large"],
            "registered_age": user["registered"]["age"]
        }
        


def insert_voters(con, curr, voter_data):
    curr.execute("""
        INSERT INTO voters (
            voter_id, voter_name, date_of_birth, gender, nationality, 
            registration_number, address_street, address_city, 
            address_state, address_country, address_postcode, 
            email, phone_number, cell_number, picture, registered_age
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        voter_data["voter_id"],
        voter_data["voter_name"],
        voter_data["date_of_birth"],
        voter_data["gender"],
        voter_data["nationality"],
        voter_data["registration_number"],
        voter_data["address_street"],
        voter_data["address_city"],
        voter_data["address_state"],
        voter_data["address_country"],
        voter_data["address_postcode"],
        voter_data["email"],
        voter_data["phone_number"],
        voter_data["cell_number"],
        voter_data["picture"],
        voter_data["registered_age"]
    ))
    conn.commit()
    print("Data inserted successfully.")




if __name__ == '__main__':
    producer = kafka.SerializingProducer({'bootstrap.servers' : 'localhost:9092'})
    try:
        conn = mysql.connector.connect(
            host= host,
            user= user,
            password= password,
            database=db
                )
        curr = conn.cursor()
        create_table(conn , curr)

        curr.execute("""
            Select * from candidates
        """)
        candidates = curr.fetchall()
        # print(candidates)

        if len(candidates) == 0:
            for i in range(5):
                candidate = generate_candidate_data(i, 5)
                curr.execute("""
                    insert into candidates( 
                    candidate_id,
                    candidate_name,
                    party_affiliation,
                    biography,
                    campaign_platform,
                    photo_url ) values (
                        %s , %s , 
                        %s , %s , 
                        %s , %s 
                    )
                """ , 
                            (
                                candidate['candidate_id'],
                                candidate['candidate_name'],
                                candidate['party_affiliation'],
                                candidate['biography'],
                                candidate['campaign_platform'],
                                candidate['photo_url'] )
                            )

        conn.commit()


        for i in range(2):
            voter_data = gen_voter_data()
            insert_voters(conn , curr, voter_data)
            
            producer.produce(
                topic = 'voters_topic',
                key = voter_data['voter_id'],
                value = simplejson.dumps(voter_data)
            )
            print(f'produced voter')
            # print(f'produced voter {i+1}')

            producer.flush()
            
            
        
    except Exception as e:
        print(f'err in main : {e}')
    
    




