import os
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from bs4 import BeautifulSoup
import re
import pandas as pd
import datetime

from ..config import OUTPUTS_DIR, COMPILED_OUTPUTS_DIR, UTILS_DIR

pattern = re.compile(r'window\["_csrf_"\] = "([^"]+)"')


def retry(retries=4):
    def decorator_retry(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"Attempt {attempt + 1} failed: {e}")
                    last_exception = e
            raise last_exception

        return wrapper

    return decorator_retry


class CSIN_Client:
    """
    Object to make sure we get web "States" needed to parse the webpage without errors
    Using a Class should be make it more organized.
    """

    def __init__(
        self,
        csrt_token="111111111111111",
        username="username",
        password="password",
        login_url="https://coess.io/app/user/usersupport/Login",
    ):
        self.csrt_token = csrt_token
        self.username = username
        self.password = password
        self.login_url = login_url
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.Session()
        self.log_in()

    @retry(retries=4)
    def log_in(self):
        """
        Take self.Session to a logged in state preserving the csrf token needed for the headers, etc.

        Args:
            None

        Returns:
            None: The function logs us into the CSIN  .
        """
        self.payload = {"csrt": self.csrt_token}

        self.response = self.session.get(
            self.login_url, data=self.payload, auth=self.auth, verify=False
        )
        self.soup = BeautifulSoup(self.response.text, "html.parser")
        self.csrf_token = pattern.search(
            self.soup.find_all("script", {"type": True})[0].string
        ).group(1)

        self.jwt_token = (
            self.soup.find("td", text="jwtToken").find_next_sibling("td").text
        )
        self.headers = {
            "Authorization": f"Bearer {self.jwt_token}",
            "X-Security-Csrf-Token": self.csrf_token,
        }
        try:
            status = self.soup.find("td", string="status").find_next_sibling().text
            print(f"Logging in: {status}")
        except:
            print("Loggin in: Error")

    @retry(retries=4)
    def get_planta_diario_reporte(self, month: int, year: int):
        """
        Getting the data given diario by the Plantas.

        Args:
            month (int): 2 digits, up to 12.
                example: 01, 02, ... , 12
            year (int): year to parse.
                example: 2023, 2024

        Returns:
            None: The function writes output files in the '/outputs/' folder.
        """
        self.month = str(month).zfill(2)
        self.year = str(year)
        self.plantas_diario = "https://coess.io/app/reportes/GetplantaMonthReport"
        self.payload = {
            "pageNumber": "0",
            "pageSize": "100000",
            "month": self.month,
            "objetoId": "",
            "year": self.year,
            "usuarios": "",
            "csrt": self.csrt_token,
        }

        self.response_planta = self.session.get(
            self.plantas_diario, params=self.payload, headers=self.headers, verify=False
        )
        self.plantas_soup = BeautifulSoup(self.response_planta.text, "html.parser")

        summary_page = 1
        input_size = 3

        aux = pd.read_html(self.plantas_soup.find("table").prettify())
        num_of_inputs = len(aux[summary_page][0].str.split("  ", expand=True))
        total_inputs = num_of_inputs * input_size
        start = summary_page + 1

        rows_base = [x for x in range(start, start + total_inputs, input_size)]

        df = pd.DataFrame()

        for row in rows_base:
            tempo = aux[row].T
            tempo.columns = tempo.loc[0, :]
            tempo = tempo.drop(index=0)
            tempo = tempo.drop(columns=["objetoDetails", "submittedBy"])

            tempo2 = aux[row + 1].T
            tempo2.columns = tempo2.loc[0, :]
            tempo2 = tempo2.drop(index=0)

            tempo = pd.concat([tempo, tempo2], axis=1)

            df = pd.concat([df, tempo]).reset_index(drop=True)

        df.to_csv(
            os.path.join(OUTPUTS_DIR, f"plantas_diario_{self.year}_{self.month}.csv")
        )
        print(
            f"Saved diario Table: /outputs/plantas_diario_{self.year}_{self.month}.csv"
        )

    @retry(retries=4)
    def get_planta_mensual_reporte_list(self):
        """
        Get the list of all the tables in Plantas Mensual Desempeno.

        Returns:
            None: The function writes output files in the current folder.
        """

        self.plantas_mensual_list = (
            "https://coess.io/app/reportes/mensualperformance/MensualDesempeno"
        )

        self.payload = {
            "pageNumber": "0",
            "pageSize": "100000",
            "fromMonthYear": "2023-01-01",
            "toMonthYear": datetime.date.today().strftime("%Y-%m-%d"),
            "usuarios": "",
            "objetoId": "",
            "csrt": self.csrt_token,
        }

        self.response_list = self.session.get(
            self.plantas_mensual_list,
            params=self.payload,
            headers=self.headers,
            verify=False,
        )
        self.soup_list = BeautifulSoup(self.response_list.text, "html.parser")

        summary_page = 1
        input_size = 2

        aux = pd.read_html(self.soup_list.find("table").prettify())
        num_of_inputs = len(aux[summary_page][0].str.split("  ", expand=True))
        total_inputs = num_of_inputs * input_size
        start = summary_page + 1

        rows_base = [x for x in range(start, start + total_inputs, input_size)]

        self.summary = pd.DataFrame()

        for row in rows_base:
            tempo = aux[row].T
            tempo.columns = tempo.loc[0, :]
            tempo = tempo.drop(index=0).drop(
                columns=["submittedBy", "assesmentDueDate"]
            )
            self.summary = pd.concat([self.summary, tempo]).reset_index(drop=True)

        self.summary.to_csv(os.path.join(UTILS_DIR, "mensual_performance_datalist.csv"))
        print(f"Saved planta Mensual Desempeno IDs table")

    @retry(retries=4)
    def get_planta_mensual_reporte(self, id_: str, electricidad: bool, aux: bool):
        """
        Getting the Mensual Desempeno data by ID >> these can be retrieved using the method: get_planta_mensual_reporte_list().

        Args:
            id_ (str): id for a company/month/year.
            electricidad (bool): Company has electricidad Plants?
            aux (bool): Company has aux Plants?

        Returns:
            None: The function writes output files in the '/outputs/' folder.
        """

        self.id_ = id_
        self.plantas_mensual = f"https://coess.io/app/reportes/mensualperformance/MensualDesempeno/{self.id_}"
        self.payload = {"csrt": self.csrt_token}

        self.response_perf = self.session.get(
            self.plantas_mensual,
            params=self.payload,
            headers=self.headers,
            verify=False,
        )
        self.perf_soup = BeautifulSoup(self.response_perf.text, "html.parser")

        number_of_reportes = electricidad + aux
        counter = 0

        while counter < number_of_reportes:

            if electricidad == False:
                first = "aux"
            else:
                first = "electricidad"

            summary_page = 4
            input_size = 1

            aux = pd.read_html(self.perf_soup.find("table").prettify())
            num_of_inputs = len(aux[summary_page][0].str.split("  ", expand=True))
            total_inputs = num_of_inputs * input_size
            start = summary_page + 1

            rows_base = [x for x in range(start, start + total_inputs, input_size)]

            df = pd.DataFrame()

            for row in rows_base:
                tempo = aux[row].T
                tempo.columns = tempo.loc[0, :]
                tempo = tempo.drop(index=0)

                df = pd.concat([df, tempo]).reset_index(drop=True)

            df["usuarioId"] = aux[1].loc[1, 1]
            df["usuarioName"] = aux[1].loc[2, 1]
            df["reporteDate"] = aux[1].loc[4, 1]

            df.to_csv(
                os.path.join(OUTPUTS_DIR, f"plantas_mensual_{self.id_}_{first}.csv")
            )
            print(
                f"Saved planta {first.capitalize()} Mensual Perf. Table: /outputs/plantas_mensual_{self.id_}_{first}.csv"
            )
            counter += 1

            if counter >= number_of_reportes:
                break

            summary_page = start + total_inputs
            input_size = 1

            aux = pd.read_html(self.perf_soup.find("table").prettify())
            num_of_inputs = len(aux[summary_page][0].str.split("  ", expand=True))
            total_inputs = num_of_inputs * input_size
            start = summary_page + 1

            rows_base = [x for x in range(start, start + total_inputs, input_size)]

            df_2 = pd.DataFrame()

            for row in rows_base:
                tempo = aux[row].T
                tempo.columns = tempo.loc[0, :]
                tempo = tempo.drop(index=0)

                df_2 = pd.concat([df_2, tempo]).reset_index(drop=True)

            df_2["usuarioId"] = aux[1].loc[1, 1]
            df_2["usuarioName"] = aux[1].loc[2, 1]
            df_2["reporteDate"] = aux[1].loc[4, 1]

            df_2.to_csv(
                os.path.join(OUTPUTS_DIR, f"plantas_mensual_{self.id_}_aux.csv")
            )
            print(
                f"Saved planta aux Mensual Perf. Table: /outputs/plantas_mensual_{self.id_}_aux.csv"
            )
            counter += 1

    @retry(retries=4)
    def get_solar_diario_reporte(self, month: int, year: int):
        """
        Getting the data given diario by the Solar companies.

        Args:
            month (int): 2 digits, up to 12.
                example: 01, 02, ... , 12
            year (int): year to parse.
                example: 2023, 2024

        Returns:
            None: The function writes output files in the 'outputs/' folder.
        """
        self.month = str(month).zfill(2)
        self.year = str(year)
        self.solar_diario = "https://coess.io/app/reportes/ReporteSolarMensual"

        self.payload = {
            "pageNumber": "0",
            "pageSize": "100000",
            "month": self.month,
            "year": self.year,
            "csrt": self.csrt_token,
        }

        self.response_solar = self.session.get(
            self.solar_diario, params=self.payload, headers=self.headers, verify=False
        )
        self.solar_soup = BeautifulSoup(self.response_solar.text, "html.parser")

        summary_page = 1
        input_size = 2

        aux = pd.read_html(self.solar_soup.find("table").prettify())
        if pd.isna(aux[0].set_index(0).loc["output", 1]) == True:
            return
        num_of_inputs = len(aux[summary_page][0].str.split("  ", expand=True))
        total_inputs = num_of_inputs * input_size
        start = summary_page + 1

        rows_base = [x for x in range(start, start + total_inputs, input_size)]

        df = pd.DataFrame()

        for row in rows_base:
            tempo = aux[row].T
            tempo.columns = tempo.loc[0, :]
            tempo = tempo.drop(index=0)
            tempo = tempo.drop(columns=["submittedBy"])

            df = pd.concat([df, tempo]).reset_index(drop=True)

        df.to_csv(
            os.path.join(OUTPUTS_DIR, f"solar_diario_{self.year}_{self.month}.csv")
        )
        print(f"Saved Solar Table: /outputs/solar_diario_{self.year}_{self.month}.csv")

    @retry(retries=4)
    def get_pv_diario_reporte(self, month: int, year: int):
        """
        Getting the data given diario by the PV companies.

        Args:
            month (int): 2 digits, up to 12.
                example: 01, 02, ... , 12
            year (int): year to parse.
                example: 2023, 2024

        Returns:
            None: The function writes output files in the 'outputs/' folder.
        """
        self.month = str(month).zfill(2)
        self.year = str(year)
        self.pv_diario = "https://coess.io/app/reportes/GetPvMonthWiseReport"

        self.payload = {
            "pageNumber": "0",
            "pageSize": "100000",
            "month": self.month,
            "year": self.year,
            "csrt": self.csrt_token,
        }

        self.response_pv = self.session.get(
            self.pv_diario, params=self.payload, headers=self.headers, verify=False
        )
        self.pv_soup = BeautifulSoup(self.response_pv.text, "html.parser")

        summary_page = 1
        input_size = 2

        aux = pd.read_html(self.pv_soup.find("table").prettify())
        if pd.isna(aux[0].set_index(0).loc["output", 1]) == True:
            return
        num_of_inputs = len(aux[summary_page][0].str.split("  ", expand=True))
        total_inputs = num_of_inputs * input_size
        start = summary_page + 1

        rows_base = [x for x in range(start, start + total_inputs, input_size)]

        df = pd.DataFrame()

        for row in rows_base:
            tempo = aux[row].T
            tempo.columns = tempo.loc[0, :]
            tempo = tempo.drop(index=0)
            tempo = tempo.drop(columns=["submittedBy"])

            df = pd.concat([df, tempo]).reset_index(drop=True)

        df.to_csv(os.path.join(OUTPUTS_DIR, f"pv_diario_{self.year}_{self.month}.csv"))
        print(f"Saved PV Table: outputs/pv_diario_{self.year}_{self.month}.csv")

    @retry(retries=4)
    def get_hydro_diario_reporte(self, month: int, year: int):
        """
        Getting the data given diario by the hydro companies.

        Args:
            month (int): 2 digits, up to 12.
                example: 01, 02, ... , 12
            year (int): year to parse.
                example: 2023, 2024

        Returns:
            None: The function writes output files in the 'outputs/' folder.
        """
        self.month = str(month).zfill(2)
        self.year = str(year)
        self.hydro_diario = "https://coess.io/app/reportes/ReporteHydroMensual"

        self.payload = {
            "pageNumber": "0",
            "pageSize": "100000",
            "month": self.month,
            "year": self.year,
            "csrt": self.csrt_token,
        }

        self.response_hydro = self.session.get(
            self.hydro_diario, params=self.payload, headers=self.headers, verify=False
        )
        self.hydro_soup = BeautifulSoup(self.response_hydro.text, "html.parser")

        summary_page = 4
        input_size = 3

        aux = pd.read_html(self.hydro_soup.find("table").prettify())
        if pd.isna(aux[0].set_index(0).loc["output", 1]) == True:
            return
        num_of_inputs = len(aux[summary_page][0].str.split("  ", expand=True))
        total_inputs = num_of_inputs * input_size
        start = summary_page + 1

        rows_base = [x for x in range(start, start + total_inputs, input_size)]

        df = pd.DataFrame()

        for row in rows_base:
            tempo = aux[row].T
            tempo.columns = tempo.loc[0, :]
            tempo = tempo.drop(index=0)
            tempo = tempo.drop(columns=["objetoDetails", "submittedBy"])

            tempo2 = aux[row + 2].T
            tempo2.columns = tempo2.loc[0, :]
            tempo2 = tempo2.drop(index=0)

            tempo = pd.concat([tempo, tempo2], axis=1)

            df = pd.concat([df, tempo]).reset_index(drop=True)

        df.to_csv(
            os.path.join(OUTPUTS_DIR, f"hydro_diario_{self.year}_{self.month}.csv")
        )
        print(f"Saved Hydro Table: /outputs/hydro_diario_{self.year}_{self.month}.csv")
