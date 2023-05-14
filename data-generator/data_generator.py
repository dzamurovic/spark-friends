import uuid
import names
from country_list import countries_for_language
from random import randint
import datetime
import json


countries_dict = dict(countries_for_language('en'))
countries = [countries_dict[c] for c in countries_dict]

event_types = {
    1: "profile_click",
    2: "photo_view",
    3: "photo_like",
    4: "post_like",
    5: "post_comment",
    6: "post_share",
    7: "direct_message"
}

event_weights = {
    1: 10,
    2: 5,
    3: 20,
    4: 5,
    5: 5,
    6: 20,
    7: 30
}

def __create_random_date():
    start_date = datetime.date(2023, 1, 1)
    end_date   = datetime.date(2023, 5, 1)
    num_days   = (end_date - start_date).days
    rand_days   = randint(1, num_days)
    return start_date + datetime.timedelta(days=rand_days)


def __create_event(ev_type):
    return {
        "name": event_types.get(ev_type),
        "weight": event_weights.get(ev_type),
        "event_date": __create_random_date().strftime("%Y-%m-%d")
    }


def __get_country(index):
    return countries[index]


def __create_user():
    return {
        "user_id": str(uuid.uuid4()),
        "name": names.get_full_name(),
        "country": __get_country(randint(0, len(countries)-1))
    }


def generate_users(num_of_users=10):
    return [__create_user() for i in range(0, num_of_users)]


def store_users(users):
    with open("./users.json", "w") as users_f:
        for user in users:
            users_f.write(f"{json.dumps(user)}\n")


def generate_events(users, num_of_events=None):
    if num_of_events is None:
        num_of_events = len(users) ** 2
    
    events = []
    for i in range(0, num_of_events):
        u1 = randint(0, len(users)-1)
        u2 = randint(0, len(users)-1)
        while (u2 == u1):
            u1 = randint(0, len(users)-1)
        events.append({
            "source": users[u1].get("user_id"),
            "target": users[u2].get("user_id"),
            "event": __create_event(randint(1, len(event_types)-1))
        })
    return events


def store_events(events):
    with open("./events.json", "w") as events_f:
        for ev in events:
            events_f.write(f"{json.dumps(ev)}\n")



if __name__ == "__main__":
    users = generate_users()
    store_users(users)
    events = generate_events(users)
    store_events(events)




