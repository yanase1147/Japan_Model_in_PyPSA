import os, pandas as pd, time
from IPython.display import clear_output, display
from concurrent.futures import ThreadPoolExecutor
import warnings, requests, re, shutil
import matplotlib.pyplot as plt

# 発電種別カラー定義（全グラフで共通利用）
CARRIER_COLOR_MAP = {
    "Nuclear": "#745994",
    "Pumped Storage(Discharge)": "#2C3796",
    "Hydro": "royalblue",
    "Wind": "skyblue",
    "Coal": "dimgray",
    "Gas": "#E77A61",
    "Oil": "#FF0000",
    "Solar": "#EEFF00",
    "Biomass": "#228B22",
    "Other": "#D2691E",
}

# 発電量プロット
def plot_total_generation_by_carrier(network, start_date=None, end_date=None):
    """
    キャリア別の合計発電出力をプロット
    
    Args:
        network: PyPSA Network object
        start_date: 開始日時 (例: '2024-04-01' または '2024-04-01 00:00')
        end_date: 終了日時 (例: '2024-04-14' または '2024-04-14 23:00')
    """
    pd.options.plotting.backend = "matplotlib"  # プロットバックエンドをmatplotlibに
    plt.rcParams['font.family'] = 'Meiryo'  # または 'Meiryo', 'Yu Gothic' MS Gothic
    plt.rcParams['axes.unicode_minus'] = False  # マイナス符号も文字化け防止
    
    # 期間指定がある場合はデータをフィルタリング
    if start_date is not None or end_date is not None:
        mask = pd.Series(True, index=network.snapshots)
        if start_date is not None:
            mask = mask & (network.snapshots >= pd.Timestamp(start_date))
        if end_date is not None:
            mask = mask & (network.snapshots <= pd.Timestamp(end_date))
        snapshots = network.snapshots[mask]
        
        if len(snapshots) == 0:
            print(f"指定期間にデータがありません: {start_date} ~ {end_date}")
            return
        
        print(f"表示期間: {snapshots[0]} ~ {snapshots[-1]} ({len(snapshots)}時間)")
    else:
        snapshots = network.snapshots
    
    # 各generatorがどのcarrierかを取得
    carrier_map = network.generators['carrier']

    # generator名→carrier名の辞書
    gen_to_carrier = carrier_map.to_dict()

    # generators_t.pの列（generator名）ごとにcarrierでgroupbyして合計
    carrier_output = {}
    for gen, carrier in gen_to_carrier.items():
        gen_data = network.generators_t.p[gen].loc[snapshots]
        if carrier not in carrier_output:
            carrier_output[carrier] = gen_data.copy()
        else:
            carrier_output[carrier] = carrier_output[carrier] + gen_data

    # 揚水発電の出力を追加（linksから取得）
    phss_discharge = None
    phss_charge = None
    if hasattr(network, 'links') and hasattr(network, 'links_t'):
        if 'carrier' in network.links.columns:
            # carrier='Pumped Storage (Discharge)'のリンクを抽出
            phss_discharge_links = network.links[network.links['carrier'] == 'Pumped Storage(Discharge)'].index
            if len(phss_discharge_links) > 0 and hasattr(network.links_t, 'p0'):
                phss_discharge = network.links_t.p0[phss_discharge_links].loc[snapshots].sum(axis=1)
                carrier_output['Pumped Storage(Discharge)'] = phss_discharge.copy()

            # carrier='Pumped Storage (Charge)'のリンクを抽出
            phss_charge_links = network.links[network.links['carrier'] == 'Pumped Storage(Charge)'].index
            if len(phss_charge_links) > 0 and hasattr(network.links_t, 'p0'):
                phss_charge = network.links_t.p0[phss_charge_links].loc[snapshots].sum(axis=1)
                # 充電は負の値として扱う（系統からの流出なので負にする）
                phss_charge = -phss_charge

    # DataFrame化（index=時系列、columns=carrier）
    carrier_output_df = pd.DataFrame(carrier_output)

    # グラフ描画
    # キャリアの順序および色を指定（下から上に積み上がる順）
    desired_order = ["Nuclear", "Hydro", "Wind", "Coal", "Gas", "Oil", "Solar", "Pumped Storage(Discharge)", "Biomass", "Other"]
    
    # 指定順で列を並び替える（存在する列だけ抽出）
    ordered_columns = [col for col in desired_order if col in carrier_output_df.columns]
    carrier_output_df = carrier_output_df[ordered_columns]
    
    # 存在する列に対応する色のみ抽出
    colors_to_use = [CARRIER_COLOR_MAP.get(col, "gray") for col in ordered_columns]
    
    fig, ax = plt.subplots(figsize=(14, 6))
    
    # Y軸の範囲を先に計算
    y_max = network.loads_t['p_set'].loc[snapshots].sum(axis=1).max()
    y_min = 0
    if phss_charge is not None and phss_charge.sum() != 0:
        y_min = phss_charge.min()
    
    # 発電（正の値）を積み上げ
    carrier_output_df.plot.area(ax=ax, alpha=0.6, linewidth=0, stacked=True, zorder=1, color=colors_to_use)
    
    # 揚水充電（負の値）を描画（マイナス方向）
    if phss_charge is not None and phss_charge.sum() != 0:
        ax.fill_between(phss_charge.index, phss_charge, 0, 
                        alpha=0.6, color="#06A9DB", label='Pumped Storage(Charge)', zorder=2, interpolate=True)
    
    # 負荷線を最後に描画（赤色、細線）
    total_load = network.loads_t['p_set'].loc[snapshots].sum(axis=1) 
    total_load.plot(ax=ax, linewidth=1.2, color='red', label='Total Load', linestyle='-', zorder=100)
        
    # Y軸の範囲を設定
    if carrier_output_df.sum(axis=1).max() > y_max:
        y_max = carrier_output_df.sum(axis=1).max()
    margin = (y_max - y_min) * 0.05  # 5%のマージン
    ax.set_ylim(y_min - margin, y_max + margin)
    
    # 0の水平線を追加
    ax.axhline(y=0, color='black', linewidth=0.5, linestyle='-', alpha=0.5, zorder=50)
   
    plt.title("Power Generation by Carrier Type (Including Pumped Storage and Generation)")
    plt.xlabel("Time")
    plt.ylabel("Power Generation Output [MW]")
    
    # 凡例を手動で作成（全てのキャリアを表示）
    from matplotlib.patches import Patch
    
    legend_elements = []
    
    # 揚水充電を最初に追加（存在する場合）
    if phss_charge is not None and phss_charge.sum() != 0:
        legend_elements.append(Patch(facecolor='#06A9DB', alpha=0.6, label='Pumped Storage(Charge)'))
    
    # 各発電種別を順番に追加
    for carrier in desired_order:
        if carrier in carrier_output_df.columns:
            color = CARRIER_COLOR_MAP.get(carrier, "gray")
            legend_elements.append(Patch(facecolor=color, alpha=0.6, label=carrier))
    
    # Total Loadを最後に追加
    legend_elements.append(plt.Line2D([0], [0], color='red', linewidth=1.2, label='Total Load'))
    
    ax.legend(handles=legend_elements, title="Carrier", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()


# バスごとの発電量内訳プロット
def plot_generation_by_bus(network):
    import pandas as pd
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
    # 日本語フォント設定
    plt.rcParams['font.family'] = 'Meiryo'
    plt.rcParams['axes.unicode_minus'] = False
    # 発電種別の順序と色を定義
    desired_order = ["Nuclear", "Pumped Storage(Discharge)", "Hydro", "Wind", "Coal", "Gas", "Oil", "Solar", "Biomass", "Other"]
    # バスのリスト
    # データ側で Kyusyu / Kyushu の綴り揺れがあるため両方に対応
    bus_list = ['Hokkaido', 'Tohoku', 'Tokyo', 'Hokuriku', 'Chubu', 'Kansai', 'Shikoku', 'Chugoku', 'Kyusyu', 'Kyushu']
    # 各バスの発電量データを収集
    bus_generation_data = {}
    for bus_name in bus_list:
        # このバスに接続されている発電機を取得
        bus_generators = network.generators[network.generators.bus == bus_name]
        if len(bus_generators) == 0:
            continue
        # 発電機ごとの総発電量を計算
        gen_by_carrier = network.generators_t.p[bus_generators.index].sum().groupby(bus_generators.carrier).sum()
        # 揚水発電は除外
        if '揚水' in gen_by_carrier.index:
            gen_by_carrier = gen_by_carrier.drop('揚水')
        if gen_by_carrier.sum() > 0:
            bus_generation_data[bus_name] = gen_by_carrier
    # 1つのグラフに全地域の横棒グラフを作成
    fig, ax = plt.subplots(figsize=(6, len(bus_generation_data) * 0.6))

    # Y軸の位置を設定
    y_positions = range(len(bus_generation_data))
    bus_names_list = list(bus_generation_data.keys())

    # 全エリアで実際に出現したcarrierを記録（凡例用）
    carriers_present = set()

    # 各バスのデータをプロット
    for idx, (y_pos, bus_name) in enumerate(zip(y_positions, bus_names_list)):
        gen_data = bus_generation_data[bus_name]
        total_gen = gen_data.sum()
        
        # desired_orderに従って並び替え
        left = 0
        for carrier in desired_order:
            if carrier in gen_data.index and gen_data[carrier] > 0:
                carriers_present.add(carrier)
                percentage = (gen_data[carrier] / total_gen) * 100
                color = CARRIER_COLOR_MAP.get(carrier, "gray")
                
                # 横棒を描画
                ax.barh(y_pos, percentage, left=left, color=color, height=0.6, alpha=0.8)
                
                # パーセント表示（5%以上の場合のみ）
                if percentage >= 5:
                    ax.text(left + percentage/2, y_pos, f'{percentage:.1f}%', 
                        ha='center', va='center', fontsize=9, color='black')
                left += percentage

    # グラフの設定
    ax.set_xlim(0, 100)
    ax.set_ylim(-0.5, len(bus_generation_data) - 0.5)
    ax.set_xlabel('Percentage of Generation (%)', fontsize=10.5)
    ax.set_yticks(y_positions)
    ax.set_yticklabels([f'{name}\n({bus_generation_data[name].sum()/1e6:.0f} TWh)' for name in bus_names_list], fontsize=10)
    ax.set_title('Generation Mix by Area', fontsize=14, pad=15)
    # 凡例を下部中央に配置（全エリアで出現したcarrierのみ表示）
    legend_handles = [
        Patch(facecolor=CARRIER_COLOR_MAP.get(carrier, "gray"), label=carrier)
        for carrier in desired_order
        if carrier in carriers_present
    ]
    ax.legend(legend_handles, [h.get_label() for h in legend_handles],
              loc='lower center', bbox_to_anchor=(0.5, -0.25), ncol=3,
              fontsize=8, title_fontsize=10, frameon=False)
    ax.grid(axis='x', alpha=0.3)
    ax.invert_yaxis()  # 上から下に並べる

    plt.tight_layout()
    plt.show()
# --- IGNORE ---


def plot_generation_mix_in_total_in_pie_graph(network):
    # 発電電力量を発電種別毎の円グラフで表示
    import matplotlib.pyplot as plt

    # 日本語フォント設定
    plt.rcParams['font.family'] = 'Meiryo'  # Windows用
    plt.rcParams['axes.unicode_minus'] = False  # マイナス記号の文字化け防止

    desired_order = ["Nuclear", "Pumped Storage(Discharge)", "Hydro", "Wind", "Coal", "Gas", "Oil", "Solar", "Biomass", "Other"]

    # 発電種別ごとの総発電量を計算
    generation_by_carrier = network.generators_t.p.sum().groupby(network.generators.carrier).sum()

    # まずネットワークにどんなcarrierがあるか確認
    print("ネットワーク内のcarrier一覧:")
    print(generation_by_carrier.index.tolist())

    # 揚水発電は除外
    if '揚水' in generation_by_carrier.index:
        generation_by_carrier = generation_by_carrier.drop('揚水')

    # desired_orderに従って並び替え（存在するもののみ）
    ordered_generation = {}
    for carrier in desired_order:
        if carrier in generation_by_carrier.index:
            ordered_generation[carrier] = generation_by_carrier[carrier]

    # 色の対応を作成
    colors_to_use = [CARRIER_COLOR_MAP.get(carrier, "gray") for carrier in ordered_generation.keys()]

    # 円グラフを作成
    fig, ax = plt.subplots(figsize=(5, 5))
    ax.pie(ordered_generation.values(),
        labels=ordered_generation.keys(),
        autopct='%1.1f%%',
        startangle=90,
        colors=colors_to_use,
        textprops={'fontsize': 10})
    ax.set_title('Total Generation by Carrier Type', fontsize=10)
    plt.tight_layout()
    plt.show()

