// Auto-generated , DO NOT EDIT
import {Entity, FunctionPropertyNames, FieldsExpression, GetOptions } from "@subql/types-core";
import assert from 'assert';



export type WithdrawalProps = Omit<Withdrawal, NonNullable<FunctionPropertyNames<Withdrawal>>| '_name'>;

export class Withdrawal implements Entity {

    constructor(
        
        id: string,
        amount: bigint,
        blockNumber: number,
        fromId: string,
    ) {
        this.id = id;
        this.amount = amount;
        this.blockNumber = blockNumber;
        this.fromId = fromId;
        
    }

    public id: string;
    public amount: bigint;
    public blockNumber: number;
    public date?: Date;
    public fromId: string;
    

    get _name(): string {
        return 'Withdrawal';
    }

    async save(): Promise<void>{
        let id = this.id;
        assert(id !== null, "Cannot save Withdrawal entity without an ID");
        await store.set('Withdrawal', id.toString(), this);
    }

    static async remove(id:string): Promise<void>{
        assert(id !== null, "Cannot remove Withdrawal entity without an ID");
        await store.remove('Withdrawal', id.toString());
    }

    static async get(id:string): Promise<Withdrawal | undefined>{
        assert((id !== null && id !== undefined), "Cannot get Withdrawal entity without an ID");
        const record = await store.get('Withdrawal', id.toString());
        if (record) {
            return this.create(record as WithdrawalProps);
        } else {
            return;
        }
    }

    static async getByFromId(fromId: string): Promise<Withdrawal[] | undefined>{
      const records = await store.getByField('Withdrawal', 'fromId', fromId);
      return records.map(record => this.create(record as WithdrawalProps));
    }


    /**
     * Gets entities matching the specified filters and options.
     *
     * ⚠️ This function will first search cache data followed by DB data. Please consider this when using order and offset options.⚠️
     * */
    static async getByFields(filter: FieldsExpression<WithdrawalProps>[], options?: GetOptions<WithdrawalProps>): Promise<Withdrawal[]> {
        const records = await store.getByFields('Withdrawal', filter, options);
        return records.map(record => this.create(record as WithdrawalProps));
    }

    static create(record: WithdrawalProps): Withdrawal {
        assert(typeof record.id === 'string', "id must be provided");
        let entity = new this(
            record.id,
            record.amount,
            record.blockNumber,
            record.fromId,
        );
        Object.assign(entity,record);
        return entity;
    }
}
