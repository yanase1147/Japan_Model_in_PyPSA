##　TEST

import os, pandas as pd, time
from IPython.display import clear_output, display
from concurrent.futures import ThreadPoolExecutor
import warnings, requests, re, shutil
import matplotlib.pyplot as plt

# 需要データの読み込み
def import_demand_data_from_network_file(network, network_file_name, demand_change_compared_to_2024):
    demand_data_raw = pd.read_excel(network_file_name, sheet_name='Demand', index_col=0, parse_dates=True)
    
    # インデックスをdatetimeに変換
    demand_data_raw.index = pd.to_datetime(demand_data_raw.index)
    
    # 2月29日を除外（閏年対応）
    demand_data_raw = demand_data_raw[~((demand_data_raw.index.month == 2) & (demand_data_raw.index.day == 29))]
    
    # ネットワークのスナップショットの年を取得
    target_year = network.snapshots[0].year
    base_year = demand_data_raw.index[0].year
    
    # demand_data_rawに格納されたデータをnetwork.loads_t.p_setに適用
    for load in network.loads.index:
        if load in demand_data_raw.columns:
            # 2024年のデータを対象年のスナップショットに合わせて調整
            if target_year != base_year:
                # 月日時刻を保持したまま年だけを変更
                adjusted_index = demand_data_raw.index.map(lambda x: x.replace(year=target_year))
                demand_series = pd.Series(demand_data_raw[load].values, index=adjusted_index)
                # ネットワークのスナップショットに合わせてリインデックス
                demand_series = demand_series.reindex(network.snapshots, method='nearest')
            else:
                demand_series = demand_data_raw[load].reindex(network.snapshots, method='nearest')
            
            # 需要変化率を適用
            network.loads_t.p_set[load] = demand_series * (1 + demand_change_compared_to_2024 / 100)
        else:
            print(f"Warning: Load '{load}' not found in demand data.")

# 太陽光発電の時系列データをRenewable.Ninja APIから取得してCSVに保存
def GetSolarTimeSeriesData(file_name, output_file, Year_of_analysis, renewable_ninja_api_key):
    # pypsa-japan-10BusModel.xlsx のbusesのバス名と座標を取得して、年間の時系列データを取得してCSVに保存
    import pandas as pd
    import requests

    # ネットワークファイルからバス情報を読み込み
    buses_df = pd.read_excel(file_name, sheet_name='buses')
    buses_df = buses_df.set_index('name')
    
    # carrier='AC'のバスのみに絞る
    if 'carrier' in buses_df.columns:
        buses_df = buses_df[buses_df['carrier'] == 'AC']
        print(f"carrier='AC'のバスに絞り込みました: {len(buses_df)}個")

    # 座標情報を含むバス位置データフレームを作成
    bus_coords = buses_df[['y', 'x']].copy()
    bus_coords.columns = ['lat', 'lon']
    bus_coords = bus_coords.dropna()

    print(f"取得したバス数: {len(bus_coords)}")
    print(bus_coords)

    # 年間の日付範囲を作成（JSTで最終的に必要な範囲）
    annual_snapshots = pd.date_range(f"{Year_of_analysis}-01-01 00:00",
                                    f"{Year_of_analysis}-12-31 23:00",
                                    freq="h")

    # 結果を格納するDataFrame
    solar_data_annual_full = pd.DataFrame(index=annual_snapshots)

    # 各バスの座標に対してRenewable.Ninja APIからデータを取得
    for bus_name, row in bus_coords.iterrows():
        lat = row['lat']
        lon = row['lon']
        
        print(f"Fetching data for {bus_name} (lat: {lat}, lon: {lon})...")
        
        # Renewable.Ninja API リクエスト
        # JSTへの変換で9時間進むため、前日の15:00 UTCから取得開始
        # （前日の15:00 UTC = 当日の0:00 JST）
        url = 'https://www.renewables.ninja/api/data/pv'
        params = {
            'lat': lat,
            'lon': lon,
            'date_from': f'{Year_of_analysis - 1}-12-31',  # 前年の12/31から取得
            'date_to': f'{Year_of_analysis}-12-31',
            'dataset': 'merra2',
            'capacity': 1.0,
            'system_loss': 0.1,
            'tracking': 0,
            'tilt': 35,
            'azim': 180,
            'format': 'json'
        }
        headers = {'Authorization': f'Token {renewable_ninja_api_key}'}
        
        response = requests.get(url, params=params, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            # レスポンス構造を確認してデバッグ
            if isinstance(data, dict) and 'data' in data:
                # 辞書形式のレスポンス (時刻がキーの場合)
                if isinstance(data['data'], dict):
                    # Unix時間(ミリ秒)をdatetimeに変換
                    time_keys = list(data['data'].keys())
                    # キーが数値(Unix時間)かどうか確認
                    if time_keys and str(time_keys[0]).isdigit():
                        # Unix時間(ミリ秒)の場合
                        time_index = pd.to_datetime([int(k) for k in time_keys], unit='ms')
                    else:
                        # 文字列形式の場合
                        time_index = pd.to_datetime(time_keys)
                    # JSTに変換（UTC+9時間）
                    time_index = time_index.tz_localize('UTC').tz_convert('Asia/Tokyo').tz_localize(None)
                    values = list(data['data'].values())
                    # 辞書から数値を抽出 (PyPSA形式)
                    if values and isinstance(values[0], dict):
                        values = [v.get('electricity', v) if isinstance(v, dict) else v for v in values]
                    # 時刻インデックスと値を組み合わせて、annual_snapshotsの範囲にリインデックス
                    temp_series = pd.Series(values, index=time_index)
                    solar_data_annual_full[bus_name] = temp_series.reindex(annual_snapshots, fill_value=0)
                # リスト形式のレスポンス (DataFrame変換可能な場合)
                elif isinstance(data['data'], list):
                    df_temp = pd.DataFrame(data['data'])
                    # 時刻カラム名を探す
                    time_col = next((col for col in df_temp.columns if 'time' in col.lower()), None)
                    if time_col:
                        df_temp.index = pd.to_datetime(df_temp[time_col])
                    # 発電量カラム名を探す
                    elec_col = next((col for col in df_temp.columns if 'electric' in col.lower() or 'power' in col.lower()), df_temp.columns[1] if len(df_temp.columns) > 1 else df_temp.columns[0])
                    solar_data_annual_full[bus_name] = df_temp[elec_col]
            else:
                print(f"  ⚠ Unexpected response format for {bus_name}")
                solar_data_annual_full[bus_name] = 0
            print(f"  ✓ Success for {bus_name}")
        else:
            print(f"  ✗ Failed for {bus_name}: {response.status_code}")
            solar_data_annual_full[bus_name] = 0

    # PyPSA形式のCSVとして保存（数値のみ、UTF-8エンコーディング）

    solar_data_annual_full.to_csv(output_file, encoding='utf-8-sig')
    print(f"\n年間太陽光データ(PyPSA形式)を保存しました: {output_file}")
    print(f"データサイズ: {solar_data_annual_full.shape}")
    print("\n最初の5行:")
    print(solar_data_annual_full.head())
    print("\n統計情報:")
    print(solar_data_annual_full.describe())

def WindTimeSeriesDataSet(network, wind_data_file):
    # 風力発電データを読み込んで割り当て
    
    if os.path.exists(wind_data_file):
        print(f"風力データを読み込んでいます: {wind_data_file}")
        wind_data = pd.read_csv(wind_data_file, index_col=0, parse_dates=True)
        
        # インデックスをdatetimeに変換
        wind_data.index = pd.to_datetime(wind_data.index)
        
        # 2月29日を除外（閏年対応）
        wind_data = wind_data[~((wind_data.index.month == 2) & (wind_data.index.day == 29))]
        
        # ネットワークのスナップショットの年を取得
        target_year = network.snapshots[0].year
        base_year = wind_data.index[0].year
        
        # 風力発電機を抽出（carrierが'wind'または'風力'のもの）
        wind_gens = network.generators[network.generators.carrier.str.contains('wind|風力', case=False, na=False)]
        
        # 各風力発電機にバスのデータを割り当て
        for gen_name in wind_gens.index:
            bus_name = network.generators.loc[gen_name, 'bus']
            if bus_name in wind_data.columns:
                # 2024年のデータを対象年のスナップショットに合わせて調整
                if target_year != base_year:
                    # 月日時刻を保持したまま年だけを変更
                    adjusted_index = wind_data.index.map(lambda x: x.replace(year=target_year))
                    gen_series = pd.Series(wind_data[bus_name].values, index=adjusted_index)
                    # snapshotの範囲に合わせてリインデックス
                    gen_data = gen_series.reindex(network.snapshots, method='nearest')
                else:
                    # snapshotの範囲に合わせてリインデックス
                    gen_data = wind_data[bus_name].reindex(network.snapshots, method='nearest')
                
                network.generators_t.p_max_pu[gen_name] = gen_data
            else:
                print(f"  ⚠ {gen_name} のバス {bus_name} がCSVに見つかりません")
                network.generators_t.p_max_pu[gen_name] = 0.0
    else:
        print(f"  ✗ 風力データファイルが存在しません: {wind_data_file}")

def SolarTimeSeriesDataSet(network,solar_data_file):
    # 太陽光発電データを読み込んで割り当て
    
    if os.path.exists(solar_data_file):
        print(f"太陽光データを読み込んでいます: {solar_data_file}")
        solar_data = pd.read_csv(solar_data_file, index_col=0, parse_dates=True)
        
        # インデックスをdatetimeに変換
        solar_data.index = pd.to_datetime(solar_data.index)
        
        # 2月29日を除外（閏年対応）
        solar_data = solar_data[~((solar_data.index.month == 2) & (solar_data.index.day == 29))]
        
        # ネットワークのスナップショットの年を取得
        target_year = network.snapshots[0].year
        base_year = solar_data.index[0].year
        
        # 太陽光発電機を抽出（carrierが'solar'または'太陽光'のもの）
        solar_gens = network.generators[network.generators.carrier.str.contains('solar|太陽光', case=False, na=False)]
        
        # 各太陽光発電機にバスのデータを割り当て
        for gen_name in solar_gens.index:
            bus_name = network.generators.loc[gen_name, 'bus']
            if bus_name in solar_data.columns:
                # 2024年のデータを対象年のスナップショットに合わせて調整
                if target_year != base_year:
                    # 月日時刻を保持したまま年だけを変更
                    adjusted_index = solar_data.index.map(lambda x: x.replace(year=target_year))
                    gen_series = pd.Series(solar_data[bus_name].values, index=adjusted_index)
                    # snapshotの範囲に合わせてリインデックス
                    gen_data = gen_series.reindex(network.snapshots, method='nearest')
                else:
                    # snapshotの範囲に合わせてリインデックス
                    gen_data = solar_data[bus_name].reindex(network.snapshots, method='nearest')
                
                network.generators_t.p_max_pu[gen_name] = gen_data
            else:
                print(f"  ⚠ {gen_name} のバス {bus_name} がCSVに見つかりません")
                network.generators_t.p_max_pu[gen_name] = 0.0
    else:
        print(f"  ✗ 太陽光データファイルが存在しません: {solar_data_file}")


def HydroTimeSeriesDataSet(network, hydro_data_file):
    """
    水力発電の時系列データを読み込んで、ネットワークの発電機に割り当てる
    
    Args:
        network: PyPSA Network object
        hydro_data_file: 水力時系列データのCSVファイルパス
    """
    if os.path.exists(hydro_data_file):
        print(f"水力データを読み込んでいます: {hydro_data_file}")
        hydro_data = pd.read_csv(hydro_data_file, index_col=0, parse_dates=True)
        
        # インデックスをdatetimeに変換
        hydro_data.index = pd.to_datetime(hydro_data.index)
        
        # 2月29日を除外（閏年対応）
        hydro_data = hydro_data[~((hydro_data.index.month == 2) & (hydro_data.index.day == 29))]
        
        # ネットワークのスナップショットの年を取得
        target_year = network.snapshots[0].year
        base_year = hydro_data.index[0].year
        
        # 水力発電機を抽出（carrierが'hydro'または'水力'のもの）
        hydro_gens = network.generators[network.generators.carrier.str.contains('hydro|水力', case=False, na=False)]
        
        if len(hydro_gens) == 0:
            print("  ⚠ 水力発電機が見つかりません")
            return
        
        print(f"  水力発電機: {len(hydro_gens)}台")
        
        # 水力稼働率カラムを取得（'水力稼働率'など）
        rate_column = None
        for col in hydro_data.columns:
            if '水力' in col or 'hydro' in col.lower():
                rate_column = col
                break
        
        if rate_column is None:
            print(f"  ✗ 水力稼働率カラムが見つかりません。利用可能なカラム: {hydro_data.columns.tolist()}")
            return
        
        # 各水力発電機に稼働率データを割り当て
        for gen_name in hydro_gens.index:
            # 年の調整
            if target_year != base_year:
                # 月日時刻を保持したまま年だけを変更
                adjusted_index = hydro_data.index.map(lambda x: x.replace(year=target_year))
                gen_series = pd.Series(hydro_data[rate_column].values, index=adjusted_index)
                # snapshotの範囲に合わせてリインデックス
                gen_data = gen_series.reindex(network.snapshots, method='nearest')
            else:
                # snapshotの範囲に合わせてリインデックス
                gen_data = hydro_data[rate_column].reindex(network.snapshots, method='nearest')
            
            network.generators_t.p_max_pu[gen_name] = gen_data
            
        print(f"  ✓ {len(hydro_gens)}台の水力発電機に稼働率を設定しました")
    else:
        print(f"  ✗ 水力データファイルが存在しません: {hydro_data_file}")
