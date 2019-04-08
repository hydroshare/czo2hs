# Share resources owned by czo_XXXX accounts with their namesake Groups with specific Privilege

# Copy this script into "hydroshare" container and run from inside
# docker cp ./share2groups.py hydroshare:/tmp/share2groups.py
# docker exec -it hydroshare bash
# python manage.py shell < /tmp/share2groups.py

from hs_core.hydroshare import utils
from hs_access_control.models import PrivilegeCodes

# PrivilegeCodes.CHANGE or PrivilegeCodes.VIEW
SHARE_WITH_GROUP_PRIVILEGE = PrivilegeCodes.CHANGE

# czo_XXXXX HS account names and namesake Group names
user_group_mapping = {
    "czo_national": "CZO National",
    "czo_boulder": "CZO Boulder",
    "czo_christina": "CZO Christina",
    "czo_eel": "CZO Eel",
    "czo_catalina-jemez": "CZO Catalina-Jemez",
    "czo_reynolds": "CZO Reynolds",
    "czo_luquillo": "CZO Luquillo",
    "czo_sierra": "CZO Sierra",
    "czo_calhoun": "CZO Calhoun",
    "czo_shale-hills": "CZO Shale-Hills",
}


for u_name, g_name in user_group_mapping.items():

    try:
        print("Sharing {}'s resource with Group {} with {} privilege".format(u_name,
                                                                             g_name,
                                                                             SHARE_WITH_GROUP_PRIVILEGE))
        # user obj
        u = utils.user_from_id(u_name)

        # group obj
        g = utils.group_from_id(g_name)

        # res owned by u
        owned_resources = u.uaccess.get_resources_with_explicit_access(PrivilegeCodes.OWNER)

        for r in owned_resources:
            try:
                u.uaccess.share_resource_with_group(r, g, SHARE_WITH_GROUP_PRIVILEGE)
            except Exception as ex:
                print(ex.message)

    except Exception as ex:
        print(ex.message)


print("Done")




