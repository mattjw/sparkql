from sparkql import Struct, String, Integer


class User(Struct):
    id = Integer(name="user_id", nullable=False)
    username = String()


class Article(Struct):
    id = Integer(name="article_id", nullable=False)
    title = String()
    author = User()
    text = String(name="body")


article_a = Article.make_dict(
    id=1001,
    title="The article title",
    author=User.make_dict(
        id=440,
        username="user"
    ),
    text="Lorem ipsum article text lorem ipsum."
)

article_a_as_dict = {
    "article_id": 1001,
    "author": {
        "user_id": 440,
        "username": "user"},
    "body": "Lorem ipsum article text lorem ipsum.",
    "title": "The article title"
}


article_b = Article.make_dict(
    id=1002
)

article_b_as_dict = {
    "article_id": 1002,
    "author": None,
    "body": None,
    "title": None
}
