from sparkql import Struct, String, path_str, Array, path_col


class Address(Struct):
    post_code = String()
    city = String()


class User(Struct):
    username = String(nullable=False)
    address = Address()


class Comment(Struct):
    message = String()
    author = User(nullable=False)


class Article(Struct):
    title = String(nullable=False)
    author = User(nullable=False)
    comments = Array(Comment())


author_city_str = path_str(Article.author.address.city)
"author.address.city"

comment_usernames_str = path_str(Article.comments.e.author.username)
"comments.author.username"

comment_messages_str = path_str(Article.comments.message)
"comments.message"

author_city_col = path_col(Article.author.address.city)
comment_usernames_col = path_col(Article.comments.e.author.username)
comment_messages_col = path_col(Article.comments.message)
