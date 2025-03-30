def follower_counter(data: dict) -> dict:
    result = []
    for i, items in enumerate(data.items()):
        userId, followers = items
        result.append({
            "userId": userId,
            "numOfFollower": len(followers),
            "followers": followers
        })

    return result
