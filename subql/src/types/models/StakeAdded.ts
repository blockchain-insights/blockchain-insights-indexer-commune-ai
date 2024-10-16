// Auto-generated , DO NOT EDIT
import {Entity, FunctionPropertyNames, FieldsExpression, GetOptions } from "@subql/types-core";
import assert from 'assert';



export type StakeAddedProps = Omit<StakeAdded, NonNullable<FunctionPropertyNames<StakeAdded>>| '_name'>;

export class StakeAdded implements Entity {

    constructor(
        
        id: string,
        amount: bigint,
        blockNumber: number,
        fromId: string,
        toId: string,
    ) {
        this.id = id;
        this.amount = amount;
        this.blockNumber = blockNumber;
        this.fromId = fromId;
        this.toId = toId;
        
    }

    public id: string;
    public amount: bigint;
    public blockNumber: number;
    public date?: Date;
    public fromId: string;
    public toId: string;
    

    get _name(): string {
        return 'StakeAdded';
    }

    async save(): Promise<void>{
        let id = this.id;
        assert(id !== null, "Cannot save StakeAdded entity without an ID");
        await store.set('StakeAdded', id.toString(), this);
    }

    static async remove(id:string): Promise<void>{
        assert(id !== null, "Cannot remove StakeAdded entity without an ID");
        await store.remove('StakeAdded', id.toString());
    }

    static async get(id:string): Promise<StakeAdded | undefined>{
        assert((id !== null && id !== undefined), "Cannot get StakeAdded entity without an ID");
        const record = await store.get('StakeAdded', id.toString());
        if (record) {
            return this.create(record as StakeAddedProps);
        } else {
            return;
        }
    }

    static async getByFromId(fromId: string): Promise<StakeAdded[] | undefined>{
      const records = await store.getByField('StakeAdded', 'fromId', fromId);
      return records.map(record => this.create(record as StakeAddedProps));
    }

    static async getByToId(toId: string): Promise<StakeAdded[] | undefined>{
      const records = await store.getByField('StakeAdded', 'toId', toId);
      return records.map(record => this.create(record as StakeAddedProps));
    }


    /**
     * Gets entities matching the specified filters and options.
     *
     * ⚠️ This function will first search cache data followed by DB data. Please consider this when using order and offset options.⚠️
     * */
    static async getByFields(filter: FieldsExpression<StakeAddedProps>[], options?: GetOptions<StakeAddedProps>): Promise<StakeAdded[]> {
        const records = await store.getByFields('StakeAdded', filter, options);
        return records.map(record => this.create(record as StakeAddedProps));
    }

    static create(record: StakeAddedProps): StakeAdded {
        assert(typeof record.id === 'string', "id must be provided");
        let entity = new this(
            record.id,
            record.amount,
            record.blockNumber,
            record.fromId,
            record.toId,
        );
        Object.assign(entity,record);
        return entity;
    }
}
