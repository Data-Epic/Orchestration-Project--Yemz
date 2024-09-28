from prefect import task, flow, get_run_logger


@task(name="Weather data task")
def get_weather_data(city_name: str):
    import requests

    logger = get_run_logger()
    try:
        api_key = "a8c314cd32cb8689779878490c54845d"
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}"
        response = requests.get(url)
        # status code 200 is successful
        if response.status_code == 200:
            weather_data = response.json()
            return weather_data
        elif response.status_code == 404:
            print("City not found")
        elif response.status_code == 401:
            print("Invalid api key")

    except requests.exceptions.ConnectionError:
        print("Connection error, please check internet connection")

    except requests.exceptions.Timeout:
        print("Requests timed out")

    except Exception as e:
        logger.error(f"Error fetching weather data: {e}")
        raise

@task(name="Google sheets task")
def sore_weather_data_in_google_sheet(weather_data: dict):
    import gspread
    import sys
    from google.oauth2.service_account import Credentials

    logger = get_run_logger(

    )
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets',
                  'https://www.googleapis.com/auth/drive']
        json_file = r"C:\Users\ayemi\Downloads\resolute-button-436012-j3-c8d859cdaa54.json"
        credentials = Credentials.from_service_account_file(json_file, scopes=scopes)
        client = gspread.authorize(credentials)
        sheet = client.open("Weather_Data").sheet1

        city = weather_data["name"]
        city_temp = weather_data["main"]["temp"]
        city_humidity_level = weather_data["main"]["humidity"]
        city_wind_speed = weather_data["wind"]["speed"]
        city_weather_descrp = weather_data["weather"][0]["description"]

        sheet.append_row([city, city_temp, city_humidity_level, city_wind_speed, city_weather_descrp])
    except gspread.exceptions.APIError:
        print("Failed to connect too Google Sheets API")
        sys.exit(1)

    except FileNotFoundError:
        print("Json credential file not found")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Error storing weather data in Google sheets: {e}")
        raise


@flow(name="Flow_1")
def transfer_weather_data_to_google_sheet(city_name: str):
    weather_data = get_weather_data(city_name)
    sore_weather_data_in_google_sheet(weather_data)


if __name__ == "__main__":
    transfer_weather_data_to_google_sheet("London")

