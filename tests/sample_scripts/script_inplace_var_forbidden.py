a = 0

# inplace var is forbidden in RestrictedPython.
# A custom implementation can be provided, for example:
# https://github.com/zopefoundation/AccessControl/blob/f8c05c03556da188ec8331fc281f3595e9170b57/src/AccessControl/ZopeGuards.py#L641
# We can live without in place var in our scripts, so we are not providing one at the moment.
a += 1
