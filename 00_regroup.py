import sys

from setup import *
from configure import *

STP_DATE = sys.argv[1]

# an estimation of time-consuming: 9 minutes
print("| {} | Begin to regroup security data |".format(dt.datetime.now()))
calendar_cne = CCalendar(os.path.join(CALENDAR_DIR, "cne_calendar.csv"))

md_by_trade_date = []
for trade_date in calendar_cne.get_iter_list(t_bgn_date=BGN_DATE, t_stp_date=STP_DATE, t_ascending=True):
    src_md_file = "{}.cne.md.csv.gz".format(trade_date)
    src_md_money_file = "{}.cne.md.money.csv.gz".format(trade_date)
    src_mkt_cap_file = "{}.cne.mkt_cap.csv.gz".format(trade_date)
    src_to_rto_file = "{}.cne.to_rto.csv.gz".format(trade_date)

    src_md_path = os.path.join(EQUITY_SECURITY_MKT_DATA_DIR, trade_date[0:4], trade_date, src_md_file)
    src_md_money_path = os.path.join(EQUITY_SECURITY_MKT_DATA_DIR, trade_date[0:4], trade_date, src_md_money_file)
    src_mkt_cap_path = os.path.join(EQUITY_SECURITY_MKT_DATA_DIR, trade_date[0:4], trade_date, src_mkt_cap_file)
    src_to_rto_path = os.path.join(EQUITY_SECURITY_MKT_DATA_DIR, trade_date[0:4], trade_date, src_to_rto_file)

    src_md_df = pd.read_csv(src_md_path)
    src_md_money_df = pd.read_csv(src_md_money_path)
    src_mkt_cap_df = pd.read_csv(src_mkt_cap_path)
    src_to_rto_df = pd.read_csv(src_to_rto_path)

    if len(src_md_df) != len(src_md_money_df):
        print("| {} | md loaded, num error | {} |".format(trade_date, dt.datetime.now()))

    trade_date_df = pd.merge(
        left=src_md_df,
        right=src_md_money_df,
        on="wind_code",
        how="left"
    )
    trade_date_df = trade_date_df.merge(right=src_mkt_cap_df, on="wind_code", how="left")
    trade_date_df = trade_date_df.merge(right=src_to_rto_df, on="wind_code", how="left")

    trade_date_df["trade_date"] = trade_date
    md_by_trade_date.append(trade_date_df)
    # print("| {} | md loaded | {} |".format(trade_date, dt.datetime.now()))

# concat to a huge dataFrame
md_hist_df = pd.concat(md_by_trade_date, axis=0, ignore_index=True)  # type:pd.DataFrame
print("| {} | All security data loaded. |".format(dt.datetime.now()))

# regroup
for stock, stock_df in md_hist_df.groupby(by="wind_code"):
    stock_hist_df = stock_df.drop(axis=1, labels="wind_code")  # type:pd.DataFrame
    stock_hist_df = stock_hist_df.sort_values(by="trade_date", ascending=True)

    stock_hist_bgn_date = stock_hist_df["trade_date"].iloc[0]
    stock_hist_end_date = stock_hist_df["trade_date"].iloc[-1]
    stock_hist_stp_date = calendar_cne.get_next_date(stock_hist_end_date, 1)
    delta_days = (dt.datetime.strptime(STP_DATE, "%Y%m%d") - dt.datetime.strptime(stock_hist_stp_date, "%Y%m%d")).days
    if delta_days > 20:
        stock_hist_dates = calendar_cne.get_iter_list(t_bgn_date=stock_hist_bgn_date, t_stp_date=stock_hist_stp_date, t_ascending=True)
    else:
        stock_hist_dates = calendar_cne.get_iter_list(t_bgn_date=stock_hist_bgn_date, t_stp_date=STP_DATE, t_ascending=True)

    stock_num = len(stock_hist_df)
    hist_num = len(stock_hist_dates)

    save_df = pd.merge(
        left=pd.DataFrame({"trade_date": stock_hist_dates}),
        right=stock_hist_df,
        on="trade_date",
        how="left"
    )

    save_df["sn"] = save_df["trade_date"].map(calendar_cne.get_sn)
    save_df[["volume", "pct_chg", "money", "to_rto"]] = save_df[["volume", "pct_chg", "money", "to_rto"]].fillna(0)
    save_df[["close", "mkt_cap"]] = save_df[["close", "mkt_cap"]].fillna(method="ffill")
    save_df[["open", "high", "low", "close"]] = save_df[["open", "high", "low", "close"]].fillna(method="bfill", axis=1)

    # consider the day before the BGN_DATE' nav as 1.000000
    save_df["nav"] = np.cumprod(save_df["pct_chg"] / CONST_RETURN_SCALE + 1)

    # check debug
    if hist_num != stock_num:
        # a few stocks, such as 300090.SZ, 300156.SZ, 600069.SH and 600175.SH, which has been
        # removed from the market just less than 20 trade dates before the STP date would
        # generate this warning.
        print("| Warning | {} | stock_num = {:>4d} | hist num = {:>4d} |".format(stock, stock_num, hist_num))

    save_file = "{}.md.csv.gz".format(stock)
    save_path = os.path.join(MD_BY_SEC_ID_DIR, save_file)
    save_df.to_csv(save_path, index=False, float_format="%.6f", compression="gzip")
    # print("| {} | saved | {} |".format(stock, dt.datetime.now()))

print("| {} | All security data regrouped. |".format(dt.datetime.now()))
