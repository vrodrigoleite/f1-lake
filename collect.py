# %%
import fastf1
import pandas as pd
pd.set_option('display.max_columns', None)
# %%

class Collect:
    def __init__(self, year = [2021, 20222], modes = ['R', 'Q', 'S']):

        self.year = year
        self.modes = modes

    def get_data(self, year, gp, mode) -> pd.DataFrame:
        try:
            session = fastf1.get_session(year, gp, mode)
            session._load_drivers_results()
            return session.results
        except Exception as e:
            print(f"Erro ao coletar dados do GP {gp}: {e}")
            # Retorna um DataFrame vazio em caso de erro
            return pd.DataFrame() 
        
    def save_data(self, data: pd.DataFrame, year, gp, mode):
        try:
            data.to_parquet(f'data/{year}_{gp:02d}_{mode}.parquet')
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

# %%

collect = Collect([2021, 2022], ['R'])

# %%

collect.get_data(2021, 23, 'R')
collect

# %%
for i in range(1,50):

    try:
        print("Coletando dados do GP: ", i)
        # Define a sessão  de busca e carrega os dados
        session = fastf1.get_session(2021, i, 'R')
        session._load_drivers_results()
        # Exibe e salva os dados da sessão em um arquivo Parquet
        session.results
        session.results.to_parquet(f'data/2021_{i:02d}_R.parquet')

        print(session.results)

    except Exception as e:
        print(f"Erro ao coletar dados do GP {i}: {e}")
        break