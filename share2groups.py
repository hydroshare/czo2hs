# Share resources owned by czo_XXXX accounts with their namesake Groups with specific Privilege

# Run this bash script:
# ./share2groups.sh

# OR go through the following 3 steps
# docker cp ./share2groups.py hydroshare:/tmp/share2groups.py
# docker cp ./settings.py hydroshare:/tmp/settings.py
# docker exec -it hydroshare bash -c "cd /tmp; python /hydroshare/manage.py shell < share2groups.py"

from hs_core.hydroshare import utils
from hs_access_control.models import PrivilegeCodes
from settings import CZO_ACCOUNTS

# PrivilegeCodes.CHANGE or PrivilegeCodes.VIEW
SHARE_WITH_GROUP_PRIVILEGE = PrivilegeCodes.VIEW

success_counter = 0
failure_counter = 0

for account in CZO_ACCOUNTS:

    u_name = account["uname"]
    czo = account["czo"]
    if czo.lower() == "default":
        continue
    g_name = "{}".format(account["group"])

    try:
        print("Sharing {}'s resource with Group {} with privilege code {}".format(u_name,
                                                                             g_name,
                                                                             SHARE_WITH_GROUP_PRIVILEGE))
        # user obj
        u = utils.user_from_id(u_name)

        # group obj
        g = utils.group_from_id(g_name)

        # res owned by u
        owned_resources = u.uaccess.get_resources_with_explicit_access(PrivilegeCodes.OWNER)

        czo_counter = 0
        for r in owned_resources:
            try:
                czo_counter += 1
                u.uaccess.share_resource_with_group(r, g, SHARE_WITH_GROUP_PRIVILEGE)
                success_counter += 1
            except Exception as ex:
                failure_counter += 1
                print(ex.message)
        print("Processed {} res of CZO {}".format(czo_counter, u_name))

    except Exception as ex:
        failure_counter += 1
        print(ex.message)


print("Done! Total: {}; Success: {}; Failed: {}".format(success_counter+failure_counter,
                                                        success_counter,
                                                        failure_counter))




