from keycloak import KeycloakOpenID
import os


class FerrisOIDC:
    instance = None

    def get_instance(self):
        if not self.instance:
            self.instance = FerrisKeycloak()

        return self.instance


class FerrisKeycloak:

    def __init__(self):
        self._keycloak_openid = KeycloakOpenID(
            server_url=os.environ.get('KEYCLOAK_NETWORK_HOSTNAME'),
            client_id=os.environ.get('KEYCLOAK_CLIENT_ID'),
            realm_name=os.environ.get('KEYCLOAK_REALM'),
            client_secret_key=os.environ.get('KEYCLOAK_CLIENT_SECRET'),
            custom_headers={"host": os.environ.get('KEYCLOAK_PUBLIC_HOSTNAME')}
        )

    def introspect(self, token):
        return self._keycloak_openid.introspect(token)

    def refresh_token(self, refresh_token):
        return self._keycloak_openid.refresh_token(refresh_token)

    def userinfo(self, token):
        return self._keycloak_openid.userinfo(token)

    def decode_token(self, token):
        return self._keycloak_openid.decode_token(token, key=os.environ.get('KEYCLOAK_REALM_PUBLIC_KEY'))


