# import functools

# class A:

#     def handler(self, fct):

#         @functools.wraps(fct)
#         def wrapper(self, *args, **kwargs):
#             return fct(self, *args, **kwargs)
#         return wrapper

#         pass


# a = A()


# @a.handler
# async def generate(request, context):
