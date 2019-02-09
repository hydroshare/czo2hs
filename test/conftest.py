"""
This is unused sample conftest code to stub out where the real fixtures will go
https://docs.pytest.org/en/latest/fixture.html
"""
import pytest


@pytest.fixture
def smtp_connection(scope="session"):
    import smtplib
    return smtplib.SMTP("smtp.gmail.com", 587, timeout=5)


def test_ehlo(smtp_connection):
    response, msg = smtp_connection.ehlo()
    assert response == 250
    assert 0  # for demo purposes
