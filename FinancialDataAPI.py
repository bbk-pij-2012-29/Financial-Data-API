import pandas as pd
import numpy as np
import os
from datetime import date, datetime, timedelta


class FinancialDataAPI:
    __data_dict = None
    __field_meta = None
    __date_format = '%Y-%m-%d'
    
    def __init__(self, source='./data', sep=';'):
        if FinancialDataAPI.__data_dict is None:
            # Load all raw data sets
            files = [f for f in os.listdir('./data') if f[0] != '.']
            FinancialDataAPI.__data_dict = {f.replace('.csv', '').replace('us-', ''): pd.read_csv('{}/{}'.format(source, f), sep=sep) for f in files}

            # Convert the date columns into the datetime64 type
            for data_set in FinancialDataAPI.__data_dict:
                df = FinancialDataAPI.__data_dict[data_set]
                for col in list(df.columns):
                    if 'date' in col.lower():
                        df[col] = df[col].astype('datetime64[ns]')
                    
        
        if FinancialDataAPI.__field_meta is None:
            # load fields metadata
            FinancialDataAPI.__field_meta = pd.read_csv('./meta/fields-meta.csv').fillna('')
    
    
    def reload_data_sets_and_meta(self, source='./data', sep=';'):
        """
            The function reloads the raw data sets and data meta from the drive.
        """
        
        # Load all raw data sets
        files = [f for f in os.listdir('./data') if f[0] != '.']
        FinancialDataAPI.__data_dict = {f.replace('.csv', '').replace('us-', ''): pd.read_csv('{}/{}'.format(source, f), sep=sep) for f in files}

        # Convert the date columns into the datetime64 type
        for data_set in FinancialDataAPI.__data_dict:
            df = FinancialDataAPI.__data_dict[data_set]
            for col in list(df.columns):
                if 'date' in col.lower():
                    df[col] = df[col].astype('datetime64[ns]')
        
        # load fields metadata
        FinancialDataAPI.__field_meta = pd.read_csv('./meta/fields-meta.csv').fillna('')
    
    
    def list_fields(self):
        """
            The function shows the full list of fields
        """
        
        df = FinancialDataAPI.__field_meta[['Long Name', 'Short Name', 'func', 'doc']].copy()
        df['func'] = df['func'].str[4:]
        df = df.rename(columns={'func': 'Category', 'doc': 'Quick Document'})
        
        return df.copy()
    
    
    def list_data_category(self):
        """
            The function lists all the data categories available
        """
        
        return [cat.replace('get_', '') for cat in FinancialDataAPI.__field_meta['func'].unique().tolist()]
        
    
    def list_fields_by_category(self, category_list):
        """
            The function returns a list of fields for a given category list
            category_list: list of categories
        """
        
        category_list = [f'get_{cat}' for cat in category_list]
        df = FinancialDataAPI.__field_meta[FinancialDataAPI.__field_meta['func'].isin(category_list)]
        df = df[['Long Name', 'Short Name', 'func', 'doc']]
        df['func'] = df['func'].str[4:]
        df = df.rename(columns={'func': 'Category', 'doc': 'Quick Document'})
        
        return df.copy()
    
    
    def list_data_sets(self):
        """
            The function retuns a list of names of the raw data sets.
        """
        
        return list(FinancialDataAPI.__data_dict.keys())
    
    
    def get_data_set(self, data_set):
        """
            The function returns raw data set for a given name of the data set.
        """
        
        return FinancialDataAPI.__data_dict[data_set]
    
    
    def get_classification(self, level='Sector'):
        """
            level: Sector (level 1), Industry (level 2)
        """
        
        df = FinancialDataAPI.__data_dict['industries']
        level = level.title().strip()
        return df[level].unique().tolist()
    
    
    def get_all_tickers(self, as_of_date=date.today()):
        """
            The function returns a list of tickers for a given as of date.
            The list only contains the valid tickers.
            i.e. the tickers must have valid price on or before the as of date.
        """
        
        num_days_check = 5 # number of days to validate if we consider the stock was active
        
        px_df = FinancialDataAPI.__data_dict['shareprices-daily']
        
        # check if price exist
        tickers_valid = px_df[
            (px_df['Date'] <= as_of_date.strftime(FinancialDataAPI.__date_format))
            & (px_df['Date'] >= (as_of_date - timedelta(days=num_days_check)).strftime(FinancialDataAPI.__date_format))
        ]['Ticker'].unique().tolist()
        
        return tickers_valid
    
    
    def get_ticker_by_classification(self, in_, level='Sector', as_of_date=date.today()):
        """
            in_: List of sectors or industries
            level: Sector (level 1), Industry (level 2)
            as_of_date: datetime.date
        """
        
        num_days_check = 5 # number of days to validate if we consider the stock was active
        
        company_df = FinancialDataAPI.__data_dict['companies']
        industry_df = FinancialDataAPI.__data_dict['industries']
        px_df = FinancialDataAPI.__data_dict['shareprices-daily']
        
        level = level.title().strip()
        industry_ids = industry_df[industry_df[level].isin(in_)]['IndustryId'].unique().tolist()
        
        tickers = company_df[company_df['IndustryId'].isin(industry_ids)]['Ticker'].unique().tolist()
        
        # check if price exist
        tickers_valid = px_df[
            (px_df['Ticker'].isin(tickers)) 
            & (px_df['Date'] <= as_of_date.strftime(FinancialDataAPI.__date_format))
            & (px_df['Date'] >= (as_of_date - timedelta(days=num_days_check)).strftime(FinancialDataAPI.__date_format))
        ]['Ticker'].unique().tolist()
        
        return tickers_valid
    
    
    def __get_field_info(self, keyword):
        """
            The function searches the field metat data given the keyword. 
            The keyword is used to match with the long and short name in lowercase.
            The return is a dataframe contains all metadata related to the fields matched.
        """
        
        field_meta_df = FinancialDataAPI.__field_meta
        search_long_name = field_meta_df['Long Name'].str.lower().str.contains(keyword.strip().lower())
        search_short_name = field_meta_df['Short Name'].str.lower().str.contains(keyword.strip().lower())
        match_df = field_meta_df[search_long_name | search_short_name]
        match_df = match_df.reset_index().copy()
        
        del match_df['index']
        
        return match_df
    
    
    def display_field_info(self, keyword):
        """
            Search by field long and short names and display anyone contains the keyword.
            keyword: str
        """
        
        match_df = self.__get_field_info(keyword)
        
        for i in range(len(match_df)):
            row = match_df.iloc[i]
            print("{long_name} ({short_name}) \n  Parameters: {params} \n  {doc} \n\n".format(
                long_name=row['Long Name'], short_name=row['Short Name'], 
                params=row['params'], doc=row['doc']
            ))
            
    
    def __get_field(self, field):
        """
            The function retunrs the field metadata as dictionary for the given field name.
            The name can either be long or short name. Not case sensitive.
        """
        
        field_meta_df = FinancialDataAPI.__field_meta
        search_long_name = field_meta_df['Long Name'].str.lower() == field.strip().lower()
        search_short_name = field_meta_df['Short Name'].str.lower() == field.strip().lower()
        match_df = field_meta_df[search_long_name | search_short_name]
        
        if len(match_df) == 1:
            return match_df.iloc[0].to_dict()
        else:
            raise Exception('Err: Could not find exact matching field.')
    
    
    def __get_param_value(self, param_name, default_value=None, **kwargs):
        """
            The function gets the param value from the input for the given param_name -> String
            If the param is in the input, the input value is returned
            If the param is not in the input, the default value is returned
            If no default value i.e. the param is a required input, the error will be raised
        """
        
        param_list = list(kwargs.keys())
        
        param = [p for p in param_list if p.lower().strip() == param_name.lower().strip()]
        
        value = default_value
        
        if len(param):
            # always take the first found param
            param_name = param[0]
            value = kwargs[param_name]
        else:
            if default_value == None:
                raise Exception('Err: {}= is a required parameter'.format(param_name))
        
        # check if the value is string and if yes, make it lower case
        if isinstance(value, str):
            value = value.lower()
            
        # check if the value is Date for Datetime and if yes, make it yyyy-mm-dd string
        dt_format = FinancialDataAPI.__date_format
        
        if isinstance(value, date) or isinstance(value, datetime):
            value = value.strftime(dt_format)
            
        return value
    
    
    def __get_description_data(self, tickers, field_dict, **kwargs):
        """
            The function retrieves the description data 
            for a given list of tickers and a field metadata as dictionary (use __get_field).
            Only one field is allowed and field must be description data.
        """
        
        field_long_name = field_dict['Long Name']
        data_set = field_dict['data_set'].split(',')
        join_key = field_dict['join_key'].split(',')
        
        df = FinancialDataAPI.__data_dict[data_set[0]]
        
        if len(data_set) > 1:
            for i in range(1, len(data_set)):
                df = pd.merge(df, FinancialDataAPI.__data_dict[data_set[i]], how='left', on=join_key[i-1], suffixes=('', '_r'))
        
        df = df[df['Ticker'].isin(tickers)][['Ticker', field_long_name]].copy()
        df = df.set_index('Ticker')
        df = df.loc[tickers]
        
        return df
    
    
    def __expand_to_calendar_dates(self, df, tickers, start, end):
        """
            The function works with pricing and market data fucntion.
            It expands to the calendar date series from the original datafrme.
        """
        
        cols = df.columns.tolist()
        
        num_ticker = len(tickers)
        dates_array = pd.date_range(start=start, end=end).values
        num_dates = len(dates_array)
        
        ticker_index = np.sort(np.resize(tickers, num_dates * num_ticker))
        dates_index = np.resize(dates_array, num_dates * num_ticker)
        
        expand_df = pd.DataFrame(index=[ticker_index, dates_index])
        df = pd.concat([expand_df, df.set_index(['Ticker', 'Date'])], axis=1).reset_index()
        
        df.columns = cols
        
        return df
    
    
    def __get_pricing_data(self, tickers, field_dict, **kwargs):
        """
            The function retrieves the pricing data
            for a given list of tickers and a field metadata as dictionary (use __get_field).
            Only one field is allowed and field must be description data.
            
            Param:
            start -> Date = required
            end -> Date = required
            adj -> String [y/n] = y
            fill_prev -> String [y/n]
        """
        
        data_set = field_dict['data_set']
        field_long_name = field_dict['Long Name']
        
        df = FinancialDataAPI.__data_dict[data_set]
        
        start = self.__get_param_value('start', **kwargs)
        end = self.__get_param_value('end', **kwargs)
        adj = self.__get_param_value('adj', 'y', **kwargs)
        fill_prev = self.__get_param_value('fill_prev', 'n', **kwargs)
        
        start_adj = start
        
        if fill_prev == 'y':
            start_adj = (datetime.strptime(start, FinancialDataAPI.__date_format) - timedelta(days=10)).strftime(FinancialDataAPI.__date_format)
        
        df = df[(df['Ticker'].isin(tickers)) & (df['Date'] >= start_adj) & (df['Date'] <= end)].copy()
        
        if adj == 'y':
            df['Adj Factor'] = df['Adj. Close'] - df['Close']
            df[field_long_name] = df[field_long_name] + df['Adj Factor']
            
        df = df[['Ticker', 'Date', field_long_name]]
        
        df = self.__expand_to_calendar_dates(df, tickers, start_adj, end)
        
        if fill_prev == 'y':
            df = df.fillna(method='ffill')
            
        df = df[df['Date'] >= start]
        
        # make sure the ticker order is the same as the request
        df['Ticker Order'] = df['Ticker'].apply(lambda x: tickers.index(x))
        df = df.sort_values(['Ticker Order', 'Date'])
        del df['Ticker Order']
        df = df.set_index('Ticker')
        
        return df.copy()
    
    
    def __get_market_data(self, tickers, field_dict, **kwargs):
        """
            The function retrieves the market data
            for a given list of tickers and a field metadata as dictionary (use __get_field).
            Only one field is allowed and field must be description data.
            
            Param:
            start -> Date = required
            end -> Date = required
            fill_prev -> String [y/n]
        """
        
        data_set = field_dict['data_set']
        field_long_name = field_dict['Long Name']
        
        df = FinancialDataAPI.__data_dict[data_set]
        
        start = self.__get_param_value('start', **kwargs)
        end = self.__get_param_value('end', **kwargs)
        fill_prev = self.__get_param_value('fill_prev', 'n', **kwargs)
        
        start_adj = start
        
        if fill_prev == 'y':
            start_adj = (datetime.strptime(start, FinancialDataAPI.__date_format) - timedelta(days=10)).strftime(FinancialDataAPI.__date_format)
        
        df = df[(df['Ticker'].isin(tickers)) & (df['Date'] >= start_adj) & (df['Date'] <= end)].copy()
        
        df = df[['Ticker', 'Date', field_long_name]]
        
        df = self.__expand_to_calendar_dates(df, tickers, start_adj, end)
        
        if fill_prev == 'y':
            df = df.fillna(method='ffill')
            
        df = df[df['Date'] >= start]
        
        # make sure the ticker order is the same as the request
        df['Ticker Order'] = df['Ticker'].apply(lambda x: tickers.index(x))
        df = df.sort_values(['Ticker Order', 'Date'])
        del df['Ticker Order']
        df = df.set_index('Ticker')
        
        
        return df.copy()
    
    
    def __fundamental_get_raw_data(self, data_set_name, tickers, field_long_name, as_of_date):
        """
            The function gets the raw fundamental data for the given ticker, field and as of date
        """
        
        df = FinancialDataAPI.__data_dict[data_set_name]
        
        # free version of the bulk data from SimFin doesn't provide full restated history
        # if use the paid version, then use 'Restated Date' otherwise use 'Publish Date'
        df = df[(df['Ticker'].isin(tickers)) & (df['Publish Date'] <= as_of_date)].copy()
        
        df['As of Date'] = as_of_date
        df['As of Date'] = df['As of Date'].astype('datetime64')
        
        cols = df.columns.tolist()
        fixed_cols = cols[:cols.index('Restated Date') + 1] + ['As of Date']
        raw_data_cols = [c for c in fixed_cols + [field_long_name] if c != 'SimFinId']
        
        df = df[raw_data_cols].sort_values(['Ticker', 'Publish Date'])
        df = df.groupby(['Ticker', 'Report Date']).tail(1).sort_values(['Ticker', 'Report Date'])
        
        return df.copy()
    
    
    def __fundamental_fill_missing_tickers(self, df, tickers, as_of_date):
        """
            The function check if all tickers in the df index, if no means no data and will fill the ticker with NaN
        """
        
        if 'Ticker' in df.columns.tolist():
            df = df.set_index('Ticker')
        
        missing_tickers = [tk for tk in tickers if tk not in df.index.tolist()]
        if len(missing_tickers):
            for tk in missing_tickers:
                df.loc[tk] = np.NaN
                df.loc[tk, 'As of Date'] = pd.to_datetime(as_of_date, format=FinancialDataAPI.__date_format)
            
        return df.reset_index()
    
    
    def __fundamental_offset_period(self, data_set_name, tickers, field_long_name, offset_start, offset_end, as_of_date):
        """
            The function gets the fundamental data for the given offset periods
        """
        
        df = self.__fundamental_get_raw_data(data_set_name, tickers, field_long_name, as_of_date)
        
        if offset_start == offset_end:
            offset_func = lambda d: d.sort_values(['Ticker', 'Report Date']).iloc[offset_end-1]
        else:
            if offset_end < 0:
                offset_func = lambda d: d.sort_values(['Ticker', 'Report Date']).iloc[offset_start-1:offset_end+1]
            else:
                offset_func = lambda d: d.sort_values(['Ticker', 'Report Date']).iloc[offset_start-1:]
        
        df = df.groupby('Ticker').apply(offset_func)
        
        # add missing tickers
        df = self.__fundamental_fill_missing_tickers(df, tickers, as_of_date)
        
        # make sure the ticker order is the same as the request
        df['Ticker Order'] = df['Ticker'].apply(lambda x: tickers.index(x))
        df = df.sort_values(['Ticker Order', 'As of Date', 'Report Date'])
        del df['Ticker Order']
        df = df.set_index('Ticker')
        
        return df.copy()
    
    
    def __fundamental_offset_period_aod_range(self, data_set_name, tickers, field_long_name, offset_start, offset_end, as_of_date_start, as_of_date_end):
        """
            The function gets the offset period data for a given as of date range.
        """
        df = FinancialDataAPI.__data_dict[data_set_name]
        
        # free version of the bulk data from SimFin doesn't provide full restated history
        # if use the paid version, then use 'Restated Date' otherwise use 'Report Date'
        dt_range = df[
            (df['Ticker'].isin(tickers)) & 
            (df['Publish Date'] >= as_of_date_start) & 
            (df['Publish Date'] <= as_of_date_end)
        ]['Publish Date'].unique()
        
        dt_range = [pd.to_datetime(dt).strftime(FinancialDataAPI.__date_format) for dt in dt_range]
        
        # make sure we have enough data
        if len(dt_range):
        
            if as_of_date_start != dt_range[0]:
                dt_range = [as_of_date_start] + dt_range

            full_dt_range = [pd.to_datetime(dt).strftime(FinancialDataAPI.__date_format) for dt in pd.date_range(start=as_of_date_start, end=as_of_date_end)]

            df_list = []
            last_df = None

            for dt in full_dt_range:
                if dt in dt_range:
                    last_df = self.__fundamental_offset_period(data_set_name, tickers, field_long_name, offset_start, offset_end, dt)
                    df_list.append(last_df.copy())
                else:
                    last_df['As of Date'] = dt
                    df_list.append(last_df.copy())

            df = pd.concat(df_list, sort=False).reset_index()
            df['As of Date'] = pd.to_datetime(df['As of Date'], format=FinancialDataAPI.__date_format)

            # make sure the ticker order is the same as the request
            df['Ticker Order'] = df['Ticker'].apply(lambda x: tickers.index(x))
            df = df.sort_values(['Ticker Order', 'As of Date', 'Publish Date'])
            del df['Ticker Order']
            df = df.set_index('Ticker')
            
            return df.copy()
        else:
            # todo return for all tickers NA
            raise Exception('Err: No enough data.')
        
    
    def __fundamental_absolute_period_q_ttm(self, data_set_name, tickers, field_long_name, y_start, q_start, y_end, q_end, as_of_date):
        """
            The function gets the quarterly and last 12 months fundamental data for the given absolute periods
        """
            
        df = self.__fundamental_get_raw_data(data_set_name, tickers, field_long_name, as_of_date)
        
        df['Quarter'] = df['Fiscal Period'].str[-1:].astype(int)
        
        df = df[
            (df['Fiscal Year'] >= y_start) & (df['Fiscal Year'] <= y_end)
            & (df['Quarter'] >= q_start) & (df['Quarter'] <= q_end)
        ]
        
        cols = [c for c in df.columns.tolist() if c != 'Quarter']
        df = df[cols]
        
        # add missing tickers
        df = self.__fundamental_fill_missing_tickers(df, tickers, as_of_date)
        
        # make sure the ticker order is the same as the request
        df = df.reset_index()
        df['Ticker Order'] = df['Ticker'].apply(lambda x: tickers.index(x))
        df = df.sort_values(['Ticker Order', 'As of Date', 'Publish Date'])
        del df['Ticker Order']
        df = df.set_index('Ticker')
        del df['index']
        
        return df.copy()
    
    
    def __fundamental_absolute_period_a(self, data_set_name, tickers, field_long_name, y_start, y_end, as_of_date):
        """
            The function gets the annually fundamental data for the given absolute periods
        """
        
        df = self.__fundamental_get_raw_data(data_set_name, tickers, field_long_name, as_of_date)
        
        df = df[
            (df['Fiscal Year'] >= y_start) & (df['Fiscal Year'] <= y_end)
        ]
        
        # add missing tickers
        df = self.__fundamental_fill_missing_tickers(df, tickers, as_of_date)
        
        # make sure the ticker order is the same as the request
        df = df.reset_index()
        df['Ticker Order'] = df['Ticker'].apply(lambda x: tickers.index(x))
        df = df.sort_values(['Ticker Order', 'As of Date', 'Publish Date'])
        del df['Ticker Order']
        df = df.set_index('Ticker')
        del df['index']
        
        return df.copy()
    
    
    def __get_fundamental_data(self, tickers, field_dict, **kwargs):
        """
            The function retrieves the fundamental data
            for a given list of tickers and a field metadata as dictionary (use __get_field).
            Only one field is allowed and field must be description data.
            
            Param:
            Period Type: pt -> String [q/a/ttm] = ttm
            
            Offset Start: offset_start -> Int = 0
            Offset End: offset_end -> Int = 0
            
            Year Start: y_start -> Int = latest year
            Year End: y_end -> Int = latest year
            
            Quarter Start: q_start: Int [1/2/3/4] = latest quarter
            Quarter End: q_end: Int [1/2/3/4] = latest quarter
            
            As of Date Start: as_of_date_start -> Date = date.today()
            As of Date End: as_of_date_end -> Date = date.today()
        """
        
        data_set_list = field_dict['data_set'].split('/')
        data_set_dict = {d.split('-')[1]:d for d in data_set_list}
        field_long_name = field_dict['Long Name']
        
        pt = self.__get_param_value('pt', 'ttm', **kwargs)
        
        y_q_none = -1
        
        y_start = self.__get_param_value('y_start', y_q_none, **kwargs)
        y_end = self.__get_param_value('y_end', y_q_none, **kwargs)
        
        q_start = self.__get_param_value('q_start', y_q_none, **kwargs)
        q_end = self.__get_param_value('q_end', y_q_none, **kwargs)
        
        offset_start = self.__get_param_value('offset_start', 0, **kwargs)
        offset_end = self.__get_param_value('offset_end', 0, **kwargs)
        offset = offset_end
        
        if pt == 'q':
            data_set_name = data_set_dict['quarterly']
        elif pt == 'a':
            data_set_name = data_set_dict['annual']
        elif pt == 'ttm':
            data_set_name = data_set_dict['ttm']
        
        as_of_date_start = self.__get_param_value('as_of_date_start', date.today(), **kwargs)
        as_of_date_end = self.__get_param_value('as_of_date_end', date.today(), **kwargs)
        as_of_date = as_of_date_end # default as of date is the as of date end
        
        # default is only one as of date and is_as_of_date_range is False
        is_as_of_date_range = False
        
        if as_of_date_start > as_of_date_end:
            raise Exception('Err: The start as of date is larger than the end date.')
        elif as_of_date_start < as_of_date_end:
            # request only in the case of only one offset period is given
            is_as_of_date_range = True
        
        if pt == 'q' or pt == 'ttm':
            # if any of start end y and q is provided, require all or raise exception
            # if non of start end y and q is provided, go for offset
            
            if y_start != y_q_none or y_end != y_q_none or q_start != y_q_none or q_end != y_q_none:
                if y_start != y_q_none and y_end != y_q_none and q_start != y_q_none and q_end != y_q_none:
                    if is_as_of_date_range:
                        raise Exception('Err: As of date range can only be used with offset period.')
                    else:
                        # get data for the given period
                        return self.__fundamental_absolute_period_q_ttm(
                            data_set_name, tickers, field_long_name, y_start, q_start, y_end, q_end, as_of_date
                        )
                else:
                    raise Exception('Err: Missing start end year or quarter.')
        elif pt == 'a':
            # if any of start end y is provided, require all or raise exception
            # if non of start end y is provided, go for offset
            
            if y_start != y_q_none or y_end != y_q_none:
                if y_start != y_q_none and y_end != y_q_none:
                    if is_as_of_date_range:
                        raise Exception('Err: As of date range can only be used with offset period.')
                    else:
                        # get data for the given period
                        return self.__fundamental_absolute_period_a(
                            data_set_name, tickers, field_long_name, y_start, y_end, as_of_date
                        )
                else:
                    raise Exception('Err: Missing start end year.')
                    
        # get data for the given offset period
        if is_as_of_date_range:
            # request as of date range for the given offset period
            return self.__fundamental_offset_period_aod_range(
                data_set_name, tickers, field_long_name, offset_start, offset_end, as_of_date_start, as_of_date_end
            )
        else:
            return self.__fundamental_offset_period(
                data_set_name, tickers, field_long_name, offset_start, offset_end, as_of_date
            )
        
    
    def get_data(self, tickers, field, **kwargs):
        """
            The function returns the data as dataframe
            for a given list of tickers and a field name.
            Only one field is allowed.
            If field is not found, the exception will be raised.
        """
        
        # make sure the ticker list is unique
        tickers = [tk.upper().strip() for tk in list(dict.fromkeys(tickers))]
        
        field_dict = self.__get_field(field)
        
        func_dict = {
            'get_description_data': self.__get_description_data,
            'get_pricing_data': self.__get_pricing_data,
            'get_market_data': self.__get_market_data,
            'get_fundamental_data': self.__get_fundamental_data,
        }
        
        func = func_dict[field_dict['func']]
        
        return func(tickers, field_dict, **kwargs)
        