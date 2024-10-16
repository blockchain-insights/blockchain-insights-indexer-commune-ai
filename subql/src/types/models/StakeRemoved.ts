// Auto-generated , DO NOT EDIT
import {Entity, FunctionPropertyNames, FieldsExpression, GetOptions } from "@subql/types-core";
import assert from 'assert';



export type StakeRemovedProps = Omit<StakeRemoved, NonNullable<FunctionPropertyNames<StakeRemoved>>| '_name'>;

export class StakeRemoved implements Entity {

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
        return 'StakeRemoved';
    }

    async save(): Promise<void>{
        let id = this.id;
        assert(id !== null, "Cannot save StakeRemoved entity without an ID");
        await store.set('StakeRemoved', id.toString(), this);
    }

    static async remove(id:string): Promise<void>{
        assert(id !== null, "Cannot remove StakeRemoved entity without an ID");
        await store.remove('StakeRemoved', id.toString());
    }

    static async get(id:string): Promise<StakeRemoved | undefined>{
        assert((id !== null && id !== undefined), "Cannot get StakeRemoved entity without an ID");
        const record = await store.get('StakeRemoved', id.toString());
        if (record) {
            return this.create(record as StakeRemovedProps);
        } else {
            return;
        }
    }

    static async getByFromId(fromId: string): Promise<StakeRemoved[] | undefined>{
      const records = await store.getByField('StakeRemoved', 'fromId', fromId);
      return records.map(record => this.create(record as StakeRemovedProps));
    }

    static async getByToId(toId: string): Promise<StakeRemoved[] | undefined>{
      const records = await store.getByField('StakeRemoved', 'toId', toId);
      return records.map(record => this.create(record as StakeRemovedProps));
    }


    /**
     * Gets entities matching the specified filters and options.
     *
     * ⚠️ This function will first search cache data followed by DB data. Please consider this when using order and offset options.⚠️
     * */
    static async getByFields(filter: FieldsExpression<StakeRemovedProps>[], options?: GetOptions<StakeRemovedProps>): Promise<StakeRemoved[]> {
        const records = await store.getByFields('StakeRemoved', filter, options);
        return records.map(record => this.create(record as StakeRemovedProps));
    }

    static create(record: StakeRemovedProps): StakeRemoved {
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
