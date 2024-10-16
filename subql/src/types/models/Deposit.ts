// Auto-generated , DO NOT EDIT
import {Entity, FunctionPropertyNames, FieldsExpression, GetOptions } from "@subql/types-core";
import assert from 'assert';



export type DepositProps = Omit<Deposit, NonNullable<FunctionPropertyNames<Deposit>>| '_name'>;

export class Deposit implements Entity {

    constructor(
        
        id: string,
        amount: bigint,
        blockNumber: number,
        toId: string,
    ) {
        this.id = id;
        this.amount = amount;
        this.blockNumber = blockNumber;
        this.toId = toId;
        
    }

    public id: string;
    public amount: bigint;
    public blockNumber: number;
    public date?: Date;
    public toId: string;
    

    get _name(): string {
        return 'Deposit';
    }

    async save(): Promise<void>{
        let id = this.id;
        assert(id !== null, "Cannot save Deposit entity without an ID");
        await store.set('Deposit', id.toString(), this);
    }

    static async remove(id:string): Promise<void>{
        assert(id !== null, "Cannot remove Deposit entity without an ID");
        await store.remove('Deposit', id.toString());
    }

    static async get(id:string): Promise<Deposit | undefined>{
        assert((id !== null && id !== undefined), "Cannot get Deposit entity without an ID");
        const record = await store.get('Deposit', id.toString());
        if (record) {
            return this.create(record as DepositProps);
        } else {
            return;
        }
    }

    static async getByToId(toId: string): Promise<Deposit[] | undefined>{
      const records = await store.getByField('Deposit', 'toId', toId);
      return records.map(record => this.create(record as DepositProps));
    }


    /**
     * Gets entities matching the specified filters and options.
     *
     * ⚠️ This function will first search cache data followed by DB data. Please consider this when using order and offset options.⚠️
     * */
    static async getByFields(filter: FieldsExpression<DepositProps>[], options?: GetOptions<DepositProps>): Promise<Deposit[]> {
        const records = await store.getByFields('Deposit', filter, options);
        return records.map(record => this.create(record as DepositProps));
    }

    static create(record: DepositProps): Deposit {
        assert(typeof record.id === 'string', "id must be provided");
        let entity = new this(
            record.id,
            record.amount,
            record.blockNumber,
            record.toId,
        );
        Object.assign(entity,record);
        return entity;
    }
}