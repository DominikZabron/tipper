import os
import psycopg2
from decimal import Decimal

DB_CONN_STRING = os.getenv("DB_CONN_STRING")


def get_score_probabilities(fixture_id):
    conn = psycopg2.connect(DB_CONN_STRING)
    cur = conn.cursor()
    cur.execute(
        """
        select label, probability 
        from odds 
        where 
            odds_types_id = 975909 and 
            bookmaker_id = 2 and
            fixture_id = %s
        ;
        """, (fixture_id,)
    )
    data = cur.fetchall()
    cur.close()
    conn.close()
    return [{"score": score, "chance": probability/100} for score, probability in data]


sign = lambda x: x and (1, -1)[x < 0]


def estimate_points(score_probabilities):
    """
    Given score probabilities returns expected points gained for each bet.

    :param score_probabilities: tuple of dictionaries of score and chance
    :return: list
    """
    expected_points = {}

    for outcome in score_probabilities:
        chances = {
            "exact": Decimal(0),
            "goal_diff": Decimal(0),
            "side": Decimal(0),
            "same_goal": Decimal(0)
        }
        chances["exact"] += outcome["chance"]
        already_processed_scores = [outcome["score"]]
        home_goals, away_goals = [int(x) for x in outcome["score"].split(":")]
        goal_difference = home_goals - away_goals

        for next_outcome in score_probabilities:
            if next_outcome["score"] in already_processed_scores:
                continue
            _home_goals, _away_goals = [int(x) for x in next_outcome["score"].split(":")]
            if _home_goals - _away_goals == goal_difference:
                chances["goal_diff"] += next_outcome["chance"]
                already_processed_scores.append(next_outcome["score"])

        for next_outcome in score_probabilities:
            if next_outcome["score"] in already_processed_scores:
                continue
            _home_goals, _away_goals = [int(x) for x in next_outcome["score"].split(":")]
            if sign(_home_goals - _away_goals) == sign(goal_difference):
                chances["side"] += next_outcome["chance"]
                already_processed_scores.append(next_outcome["score"])

        for next_outcome in score_probabilities:
            if next_outcome["score"] in already_processed_scores:
                continue
            _home_goals, _away_goals = [int(x) for x in next_outcome["score"].split(":")]
            if home_goals == _home_goals or away_goals == _away_goals:
                chances["same_goal"] += next_outcome["chance"]
                already_processed_scores.append(next_outcome["score"])

        expected_points[outcome["score"]] = 6 * chances["exact"] + \
                                            4 * chances["goal_diff"] + \
                                            3 * chances["side"] + \
                                            chances["same_goal"]

    return sorted(expected_points.items(), key=lambda x: x[1], reverse=True)


if __name__ == '__main__':
    FIXTURE_ID = 123456
    score_probabilities = get_score_probabilities(fixture_id=FIXTURE_ID)
    print(estimate_points(score_probabilities))
