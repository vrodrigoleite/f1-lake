# %%

import fastf1
import pandas as pd
import time
import argparse
pd.set_option('display.max_columns', None)

# %%

# Testes
df = pd.read_parquet('data/2019_01_R.parquet')
df

# %%

class Collect:
    def __init__(self, year = [2021, 2022], modes = ['R', 'Q', 'S']):

        self.year = year
        self.modes = modes

    def get_data(self, year, gp, mode) -> pd.DataFrame:

        try:
            session = fastf1.get_session(year, gp, mode)
                
        except Exception as e:
            print(f"Erro ao coletar dados do GP {gp}: {e}")
            # Retorna um DataFrame vazio em caso de erro
            return pd.DataFrame() 
        
        session._load_drivers_results()
        df = session.results

        df['Year'] = session.date.year
        df['Date'] = session.date
        df['Mode'] = session.name
        df['RoundNumber'] = session.event['RoundNumber']
        df['OfficialEventName'] = session.event['OfficialEventName']
        df['EventName'] = session.event['EventName']
        df['Country'] = session.event['Country']
        df['Location'] = session.event['Location']

        return df
        
    def save_data(self, data: pd.DataFrame, year: int, gp: int, mode: str):
        try:
            filename = f'data/{year}_{gp:02d}_{mode}.parquet'
            data.to_parquet(filename, index=False)
            print(f"Dados salvos com sucesso para o GP {gp} do ano {year} no modo {mode}.")
        except Exception as e:
            print(f"Erro ao salvar dados do GP {gp}: {e}")

    def process(self, year, gp, mode):
        data = self.get_data(year, gp, mode)
        if not data.empty:
            self.save_data(data, year, gp, mode)
            return True
        else:
            return False
        
    def process_year_modes(self, year):
        for i in range(1, 50):
            for mode in self.modes:
                if not self.process(year, i, mode) and mode == 'R':
                    return
                
    def process_years(self):
        for year in self.year:
            print(f"Processando dados do ano {year}...")
            self.process_year_modes(year)
            time.sleep(5)




# %%

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--years', '-y', nargs='+', type=int, default=[2021, 2022])
    parser.add_argument('--modes', '-m', nargs='+', type=str, default=['R'])

    args = parser.parse_args()

    collect = Collect(args.years, args.modes)
    collect.process_years()
    