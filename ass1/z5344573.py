import json
import matplotlib.pyplot as plt
import pandas as pd
import sys
import os
import numpy as np
import math
import re

studentid = os.path.basename(sys.modules[__name__].__file__)


def log(question, output_df, other):
    print("--------------- {}----------------".format(question))

    if other is not None:
        print(question, other)
    if output_df is not None:
        df = output_df.head(5).copy(True)
        for c in df.columns:
            df[c] = df[c].apply(lambda a: a[:20] if isinstance(a, str) else a)

        df.columns = [a[:10] + "..." for a in df.columns]
        print(df.to_string())


def passenger_in_out_same(row):
    if row.Passengers_In > row.Passengers_Out:
        return 'IN'
    elif row.Passengers_In < row.Passengers_Out:
        return 'OUT'
    else:
        return 'SAME'


def freight_in_out_same(Freight_In, Freight_Out):
    if Freight_In > Freight_Out:
        return 'IN'
    elif Freight_In < Freight_Out:
        return 'OUT'
    else:
        return 'SAME'


def mail_in_out_same(Mail_In, Mail_Out):
    if Mail_In > Mail_Out:
        return 'IN'
    elif Mail_In < Mail_Out:
        return 'OUT'
    else:
        return 'SAME'


def question_1(city_pairs):
    """
    :return: df1
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """
    city_pairs = pd.read_csv(city_pairs)
    city_pairs['passenger_in_out'] = city_pairs.apply(passenger_in_out_same, axis=1)
    city_pairs['freight_in_out'] = city_pairs.apply(
        lambda x: freight_in_out_same(x['Freight_In_(tonnes)'], x['Freight_Out_(tonnes)']), axis=1)
    city_pairs['mail_in_out'] = city_pairs.apply(
        lambda x: freight_in_out_same(x['Mail_In_(tonnes)'], x['Mail_Out_(tonnes)']), axis=1)
    df1 = city_pairs.copy(True)
    log("QUESTION 1",
        output_df=df1[["AustralianPort", "ForeignPort", "passenger_in_out", "freight_in_out", "mail_in_out"]],
        other=df1.shape)
    return df1


def question_2(df1):
    """
    :param df1: the dataframe created in question 1
    :return: dataframe df2
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    #################################################
    df_1 = df1[["AustralianPort", "ForeignPort", "passenger_in_out", "freight_in_out", "mail_in_out"]].copy(True)
    city_pairs = df1.copy(True)
    df_1.columns = ["AustralianPort", "ForeignPort", "passenger_in_out_1", "freight_in_out_1", "mail_in_out_1"]

    PassengerInCount = df_1.groupby(["AustralianPort", city_pairs['passenger_in_out'] == 'IN'])[
        'passenger_in_out_1'].count().unstack(fill_value=0).stack().reset_index()
    df_2 = PassengerInCount[PassengerInCount['passenger_in_out'] == True]
    PassengerInCount = df_2[['AustralianPort', 0]]
    PassengerInCount.columns = ['AustralianPort', 'PassengerInCount']

    PassengerOutCount = df_1.groupby(["AustralianPort", city_pairs['passenger_in_out'] == 'OUT'])[
        'passenger_in_out_1'].count().unstack(fill_value=0).stack().reset_index()
    df_2 = PassengerOutCount[PassengerOutCount['passenger_in_out'] == True]
    PassengerOutCount = df_2[['AustralianPort', 0]]
    PassengerOutCount.columns = ['AustralianPort', 'PassengerOutCount']

    FreightInCount = df_1.groupby(["AustralianPort", city_pairs['freight_in_out'] == 'IN'])[
        'freight_in_out_1'].count().unstack(fill_value=0).stack().reset_index()
    df_2 = FreightInCount[FreightInCount['freight_in_out'] == True]
    FreightInCount = df_2[['AustralianPort', 0]]
    FreightInCount.columns = ['AustralianPort', 'FreightInCount']

    FreightOutCount = df_1.groupby(["AustralianPort", city_pairs['freight_in_out'] == 'OUT'])[
        'freight_in_out_1'].count().unstack(fill_value=0).stack().reset_index()
    df_2 = FreightOutCount[FreightOutCount['freight_in_out'] == True]
    FreightOutCount = df_2[['AustralianPort', 0]]
    FreightOutCount.columns = ['AustralianPort', 'FreightOutCount']

    MailInCount = df_1.groupby(["AustralianPort", city_pairs['mail_in_out'] == 'IN'])['mail_in_out_1'].count().unstack(
        fill_value=0).stack().reset_index()
    df_2 = MailInCount[MailInCount['mail_in_out'] == True]
    MailInCount = df_2[['AustralianPort', 0]]
    MailInCount.columns = ['AustralianPort', 'MailInCount']

    MailOutCount = df_1.groupby(["AustralianPort", city_pairs['mail_in_out'] == 'OUT'])[
        'mail_in_out_1'].count().unstack(fill_value=0).stack().reset_index()
    df_2 = MailOutCount[MailOutCount['mail_in_out'] == True]
    MailOutCount = df_2[['AustralianPort', 0]]
    MailOutCount.columns = ['AustralianPort', 'MailInCount']

    df = PassengerInCount.merge(PassengerOutCount, how='left', on='AustralianPort').merge(FreightInCount, how='left',
                                                                                          on='AustralianPort').merge(
        FreightOutCount, how='left', on='AustralianPort').merge(MailInCount, how='left', on='AustralianPort').merge(
        MailOutCount, how='left', on='AustralianPort')
    df.columns = ['AustralianPort', 'PassengerInCount', 'PassengerOutCount', 'FreightInCount', 'FreightOutCount',
                  'MailInCount', 'MailOutCount']
    df2 = df.sort_values('PassengerInCount', ascending=False)
    log("QUESTION 2", output_df=df2, other=df2.shape)
    return df2


def question_3(df1):
    """
    :param df1: the dataframe created in question 1
    :return: df3
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """
    #################################################
    # Your code goes here ...
    #################################################
    city_pairs = df1.copy(True)
    df_6 = city_pairs.groupby("Country")['Passengers_In'].sum()
    df_avg = {'Country': df_6.index, 'Passengers_in_average': df_6.values}
    df_avg = pd.DataFrame(df_avg)
    years = (city_pairs['Year'].max() - city_pairs['Year'].min() + 1) * 12
    df_avg['Passengers_in_average'] = df_avg['Passengers_in_average'].apply(lambda x: x / years)
    df_avg = df_avg.sort_values('Passengers_in_average', ascending=True)

    df_in_sum = city_pairs.groupby("Country")['Passengers_Out'].sum()
    Passengers_out_average = {'Country': df_in_sum.index, 'Passengers_out_average': df_in_sum.values}
    Passengers_out_average = pd.DataFrame(Passengers_out_average)
    Passengers_out_average['Passengers_out_average'] = Passengers_out_average['Passengers_out_average'].apply(
        lambda x: x / years)

    df_Freight_in_average = city_pairs.groupby("Country")['Freight_In_(tonnes)'].sum()
    Freight_in_average = {'Country': df_Freight_in_average.index, 'Freight_in_average': df_Freight_in_average.values}
    Freight_in_average = pd.DataFrame(Freight_in_average)
    Freight_in_average['Freight_in_average'] = Freight_in_average['Freight_in_average'].apply(lambda x: x / years)

    df_Freight_out_average = city_pairs.groupby("Country")['Freight_Out_(tonnes)'].sum()
    Freight_out_average = {'Country': df_Freight_out_average.index,
                           'Freight_out_average': df_Freight_out_average.values}
    Freight_out_average = pd.DataFrame(Freight_out_average)
    Freight_out_average['Freight_out_average'] = Freight_out_average['Freight_out_average'].apply(lambda x: x / years)

    df_Mail_in_average = city_pairs.groupby("Country")['Mail_In_(tonnes)'].sum()
    Mail_in_average = {'Country': df_Mail_in_average.index, 'Mail_in_average': df_Mail_in_average.values}
    Mail_in_average = pd.DataFrame(Mail_in_average)
    Mail_in_average['Mail_in_average'] = Mail_in_average['Mail_in_average'].apply(lambda x: x / years)

    df_Mail_out_average = city_pairs.groupby("Country")['Mail_Out_(tonnes)'].sum()
    Mail_out_average = {'Country': df_Mail_out_average.index, 'Mail_out_average': df_Mail_out_average.values}
    Mail_out_average = pd.DataFrame(Mail_out_average)
    Mail_out_average['Mail_out_average'] = Mail_out_average['Mail_out_average'].apply(lambda x: x / years)

    df3 = df_avg.merge(Passengers_out_average, how='left', on='Country').merge(Freight_in_average, how='left',
                                                                               on='Country').merge(Freight_out_average,
                                                                                                   how='left',
                                                                                                   on='Country').merge(
        Mail_in_average, how='left', on='Country').merge(Mail_out_average, how='left', on='Country')

    log("QUESTION 3", output_df=df3, other=df3.shape)
    return df3


#
#
def question_4(df1):
    """
    :param df1: the dataframe created in question 1
    :return: df4
            Data Type: Dataframe
            Please read the assignment specs to know how to create the output dataframe
    """

    #################################################
    # Your code goes here ...
    #################################################
    city_pairs = df1.copy(True)
    df = city_pairs[city_pairs['Passengers_Out'] > 0].groupby(['AustralianPort', 'Country', 'Month']).agg(
        Count=('ForeignPort', 'count')).reset_index()

    df = df[df['Count'] > 1]
    df = df.groupby(['AustralianPort', 'Country']).agg(Count=('Month', 'count')).reset_index()
    df4 = df.groupby(['Country'])['Count'].sum().reset_index().sort_values('Count', ascending=False)
    df4.columns = ['Country', 'Unique_ForeignPort_Count']
    df4 = df4.head(5)
    log("QUESTION 4", output_df=df4, other=df4.shape)
    return df4
    #
    #


def s_d_city_I(row):
    if row.In_Out == 'I':
        return row.International_City
    else:
        return row.Australian_City


def s_d_city_O(x):
    if x.In_Out == 'O':
        return x.International_City
    else:
        return x.Australian_City


def question_5(seats):
    """
    :param seats : the path to dataset
    :return: df5
            Data Type: dataframe
            Please read the assignment specs to know how to create the  output dataframe
    """
    #################################################
    # Your code goes here ...
    #################################################
    seats_csv = pd.read_csv(seats)

    seats_csv['Source_City'] = seats_csv.apply(s_d_city_I, axis=1)
    seats_csv['Destination_City'] = seats_csv.apply(s_d_city_O, axis=1)
    df5 = seats_csv
    log("QUESTION 5", output_df=df5, other=df5.shape)
    return df5


#
#
def question_6(df5):
    """
    :param df5: the dataframe created in question 5
    :return: df6
    """
    seats_csv=df5.copy(True)
    #################################################
    # Your code goes here ...
    #################################################
    df6=pd.pivot_table(seats_csv, index=['Airline', 'Year', 'Australian_City', 'International_City'],
                   values=['All_Flights', 'Max_Seats'], aggfunc='sum').sort_values(['Airline', 'Year'],
                                                                                   ascending=[True, False])
    log("QUESTION 6", output_df=df6, other=df6.shape)
    print("I selected the airline, year, Australian_City, International_City, and the total number of \n"
          "routes and the total number of Max seats for the corresponding year. This can clearly see \n"
          "the fluctuations in the number of routes of airlines in the past few years, as well as the\n"
          "fluctuations in passenger load. For example, Air Caledonie has a significant difference in \n"
          "the number of routes in 2020 and 2021, which may be due to the impact of COVID-19.New\n"
          "airlines can also choose whether to add corresponding routes based on the route data of\n"
          "airlines that have opened routes, which can help new airlines obtain greater profits.Airlines\n "
          "that have opened routes can also decide whether to increase or decrease routes based on the\n"
          "route data of previous years.")
    return df6
#

def question_7(seats, city_pairs):
    """
    :param seats: the path to dataset
    :param city_pairs : the path to dataset
    :return: nothing, but saves the figure on the disk
    """

    #################################################
    # Your code goes here ...
    #################################################
    city_pairs=pd.read_csv(city_pairs)
    seats_csv=pd.read_csv(seats)
    df_city = city_pairs[['Year', 'Month_num', 'Passengers_In', 'Passengers_Out']]
    df_city = df_city[df_city['Passengers_In'] > 0]
    df_city = df_city[df_city['Passengers_Out'] > 0]
    df_seats = seats_csv[['Year', 'Month_num', 'Port_Region', 'Max_Seats']]
    df_all = df_seats.merge(df_city, how='left', on=['Year', 'Month_num'])
    df_all = pd.pivot_table(df_all, index=['Year'], values=['Max_Seats', 'Passengers_In', 'Passengers_Out'],
                            aggfunc='sum')
    df_all['InSeatsPercent'] = df_all.apply(lambda x: (x['Passengers_In'] / x['Max_Seats']), axis=1)
    df_all['OutSeatsPercent'] = df_all.apply(lambda x: (x['Passengers_Out'] / x['Max_Seats']), axis=1)
    df_all = df_all.reset_index().sort_values('Year', ascending=True)
    x = df_all['Year']
    y1 = df_all['InSeatsPercent']
    y2 = df_all['OutSeatsPercent']
    plt.plot(x, y1, label='Passengers_In/Max_Seats')
    plt.plot(x, y2, label='Passengers_Out/Max_Seats')
    plt.legend()
    plt.title('Seat utilization rate of airlines from 2003 to 2022')
    plt.xlabel("Year")
    plt.ylabel("Passengers/Max_Seats")
    plt.xticks(x)
    plt.gca().margins(x=0)
    plt.gcf().canvas.draw()
    maxsize = 50
    m = 1
    N = len(x)
    s = maxsize / plt.gcf().dpi * N + 2 * m
    margin = m / plt.gcf().get_size_inches()[0]
    plt.gcf().subplots_adjust(left=margin, right=1. - margin)
    plt.gcf().set_size_inches(s, plt.gcf().get_size_inches()[1])
    plt.savefig("{}-Q7.png".format(studentid))
    print("--------------- {}----------------".format("QUESTION 7"))
    print("I extracted the number of passengers entering and leaving the country, and the Max_seats of all \n"
          "airlines, and calculated it in units of years. From the line chart, it can be clearly seen that \n"
          "before 2020, the airlines are basically overbooking the flight seats , until 2021, possibly due to \n"
          "fewer passengers due to COVID-19, resulting in more empty seats in that year. And the overbooking \n"
          "status was restored immediately after the epidemic ended, so I think that overbooking within a certain \n"
          "range will bring more benefits to the airlines, even if it will compensate passengers, it should still \n"
          "have an objective effect. income.")


if __name__ == "__main__":
    df1 = question_1("city_pairs.csv")
    df2 = question_2(df1.copy(True))
    df3 = question_3(df1.copy(True))
    df4 = question_4(df1.copy(True))
    df5 = question_5("seats.csv")
    df6 = question_6(df5.copy(True))
    question_7("seats.csv", "city_pairs.csv")
