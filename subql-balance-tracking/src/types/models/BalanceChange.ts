// Auto-generated , DO NOT EDIT
import {Entity, FunctionPropertyNames, FieldsExpression, GetOptions } from "@subql/types-core";
import assert from 'assert';



export type BalanceChangeProps = Omit<BalanceChange, NonNullable<FunctionPropertyNames<BalanceChange>>| '_name'>;

export class BalanceChange implements Entity {

    constructor(
        
        id: string,
        balance: bigint,
        blockNumber: number,
        event: string,
        accountId: string,
    ) {
        this.id = id;
        this.balance = balance;
        this.blockNumber = blockNumber;
        this.event = event;
        this.accountId = accountId;
        
    }

    public id: string;
    public balance: bigint;
    public blockNumber: number;
    public date?: Date;
    public event: string;
    public accountId: string;
    

    get _name(): string {
        return 'BalanceChange';
    }

    async save(): Promise<void>{
        let id = this.id;
        assert(id !== null, "Cannot save BalanceChange entity without an ID");
        await store.set('BalanceChange', id.toString(), this);
    }

    static async remove(id:string): Promise<void>{
        assert(id !== null, "Cannot remove BalanceChange entity without an ID");
        await store.remove('BalanceChange', id.toString());
    }

    static async get(id:string): Promise<BalanceChange | undefined>{
        assert((id !== null && id !== undefined), "Cannot get BalanceChange entity without an ID");
        const record = await store.get('BalanceChange', id.toString());
        if (record) {
            return this.create(record as BalanceChangeProps);
        } else {
            return;
        }
    }

    static async getByAccountId(accountId: string): Promise<BalanceChange[] | undefined>{
      const records = await store.getByField('BalanceChange', 'accountId', accountId);
      return records.map(record => this.create(record as BalanceChangeProps));
    }


    /**
     * Gets entities matching the specified filters and options.
     *
     * ⚠️ This function will first search cache data followed by DB data. Please consider this when using order and offset options.⚠️
     * */
    static async getByFields(filter: FieldsExpression<BalanceChangeProps>[], options?: GetOptions<BalanceChangeProps>): Promise<BalanceChange[]> {
        const records = await store.getByFields('BalanceChange', filter, options);
        return records.map(record => this.create(record as BalanceChangeProps));
    }

    static create(record: BalanceChangeProps): BalanceChange {
        assert(typeof record.id === 'string', "id must be provided");
        let entity = new this(
            record.id,
            record.balance,
            record.blockNumber,
            record.event,
            record.accountId,
        );
        Object.assign(entity,record);
        return entity;
    }
}
