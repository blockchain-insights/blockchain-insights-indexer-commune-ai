// Auto-generated , DO NOT EDIT
import {Entity, FunctionPropertyNames, FieldsExpression, GetOptions } from "@subql/types-core";
import assert from 'assert';



export type BalanceSetProps = Omit<BalanceSet, NonNullable<FunctionPropertyNames<BalanceSet>>| '_name'>;

export class BalanceSet implements Entity {

    constructor(
        
        id: string,
        amount: bigint,
        blockNumber: number,
        whoId: string,
    ) {
        this.id = id;
        this.amount = amount;
        this.blockNumber = blockNumber;
        this.whoId = whoId;
        
    }

    public id: string;
    public amount: bigint;
    public blockNumber: number;
    public date?: Date;
    public whoId: string;
    

    get _name(): string {
        return 'BalanceSet';
    }

    async save(): Promise<void>{
        let id = this.id;
        assert(id !== null, "Cannot save BalanceSet entity without an ID");
        await store.set('BalanceSet', id.toString(), this);
    }

    static async remove(id:string): Promise<void>{
        assert(id !== null, "Cannot remove BalanceSet entity without an ID");
        await store.remove('BalanceSet', id.toString());
    }

    static async get(id:string): Promise<BalanceSet | undefined>{
        assert((id !== null && id !== undefined), "Cannot get BalanceSet entity without an ID");
        const record = await store.get('BalanceSet', id.toString());
        if (record) {
            return this.create(record as BalanceSetProps);
        } else {
            return;
        }
    }

    static async getByWhoId(whoId: string): Promise<BalanceSet[] | undefined>{
      const records = await store.getByField('BalanceSet', 'whoId', whoId);
      return records.map(record => this.create(record as BalanceSetProps));
    }


    /**
     * Gets entities matching the specified filters and options.
     *
     * ⚠️ This function will first search cache data followed by DB data. Please consider this when using order and offset options.⚠️
     * */
    static async getByFields(filter: FieldsExpression<BalanceSetProps>[], options?: GetOptions<BalanceSetProps>): Promise<BalanceSet[]> {
        const records = await store.getByFields('BalanceSet', filter, options);
        return records.map(record => this.create(record as BalanceSetProps));
    }

    static create(record: BalanceSetProps): BalanceSet {
        assert(typeof record.id === 'string', "id must be provided");
        let entity = new this(
            record.id,
            record.amount,
            record.blockNumber,
            record.whoId,
        );
        Object.assign(entity,record);
        return entity;
    }
}
