import pandas as pd

def update_predict(db, faresheet, preds, files_index):
    preds_df = pd.DataFrame({'pred': preds, 'id': files_index})
    # update predictions
    for index, row in preds_df.iterrows():
        if row['pred'] == 1:
            predict = 'yes'
        if row['pred'] == 0:
            predict = 'no'
        db[faresheet].update_one({"_id": row["id"]},
                                 {"$set": {"predictions.Commission": predict}})
        print(row['id'], predict)
